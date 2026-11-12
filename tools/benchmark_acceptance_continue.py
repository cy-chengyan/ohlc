#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Continue isolated acceptance with concurrent cold reads, recovery, and a sustained workload."""

import argparse
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import signal
import struct
import subprocess
import time
import traceback

from benchmark_acceptance_data import space_snapshot
from benchmark_acceptance_home import Controller, owned_directory
from benchmark_home import properties
from benchmark_metrics import atomic_json, sha256


def checkpoint_snapshot(data, destination):
    """Preserve the exact stopped CURRENT records and derive the bounded WAL replay tail."""
    records = []
    for path in sorted(data.glob("CURRENT.*")):
        raw = path.read_bytes()
        shutil.copy2(path, destination / path.name)
        valid = len(raw) == 4096 and raw[:8] == b"OHLCUR6\0"
        if valid:
            stored = struct.unpack_from("<I", raw, 4092)[0]
            body = bytearray(raw)
            body[4092:] = b"\0" * 4
            crc = 0xffffffff
            for byte in body:
                crc ^= byte
                for _ in range(8):
                    crc = (crc >> 1) ^ (0x82f63b78 if crc & 1 else 0)
            valid = stored == crc ^ 0xffffffff
        item = {"path": path.name, "sha256": sha256(path), "crc_valid": valid}
        if valid:
            item.update(generation=struct.unpack_from("<Q", raw, 32)[0],
                        sequence=struct.unpack_from("<Q", raw, 40)[0],
                        wal_id=struct.unpack_from("<I", raw, 56)[0],
                        wal_offset=struct.unpack_from("<Q", raw, 64)[0])
        records.append(item)
    usable = [record for record in records if record["crc_valid"]]
    if not usable:
        raise RuntimeError("No valid CURRENT record after controlled crash")
    latest = max(usable, key=lambda record: record["generation"])
    tail = 0
    wals = []
    for path in sorted(data.glob("wal-*.log")):
        number = int(path.stem.split("-")[1])
        size = path.stat().st_size
        wals.append({"name": path.name, "bytes": size})
        if number == latest["wal_id"]:
            tail += max(0, size - latest["wal_offset"])
        elif number > latest["wal_id"]:
            tail += max(0, size - 4096)
    result = {"current_records": records, "latest": latest, "wal_files": wals,
              "wal_bytes_after_checkpoint_cursor": tail}
    atomic_json(destination / "checkpoint.json", result)
    return result


class Acceptance(Controller):
    def clone(self, name):
        target = self.data / name
        owned_directory(target)
        source = (self.args.root / "data/smoke01/c" if self.args.smoke else
                  self.args.previous / "results/c01/ohlc-data")
        label = "copy-" + name
        unit = self.launch(label, ["cp", "-a", "--reflink=auto", str(source) + "/.", str(target)], "16-23")
        self.wait(unit, label, timeout=600)
        return target

    def command(self, script, label, data, stocks, times, extra=None):
        return [str(self.args.previous / "venv/bin/python"), "tools/" + script,
                "--seed", str(self.args.previous / "seed"),
                "--binary", str(self.args.previous / "build/ohlcd"),
                "--data", str(data), "--output", str(self.output / (label + ".jsonl")),
                "--stocks", str(stocks), "--times", str(times)] + (extra or [])

    def checked_worker(self, command, label, timeout, server_required=True):
        unit = self.launch(label, command, "24-39")
        try:
            self.wait(unit, label, timeout, server_required=server_required)
            last = json.loads((self.output / (label + ".jsonl")).read_text().splitlines()[-1])
            if last.get("event") != "complete" or last.get("status") not in ("passed", "unstable"):
                raise RuntimeError(f"Acceptance worker did not finish: {last}")
            self.results.append({"stage": label, "status": last["status"]})
            atomic_json(self.output / "progress.json", self.results)
        finally:
            self.stop(unit)

    def admin_check(self, data, name, rows, stocks, times):
        label = "admin-" + name
        before = space_snapshot(data, stocks, times, strict=True)
        started = time.monotonic_ns()
        unit = self.launch(label, [str(self.args.previous / "build/ohlc-admin"),
                                  "--memory-mib", "24576", "check", str(data)], "0-15")
        self.wait(unit, label, timeout=10800)
        output = (self.output / (label + ".log")).read_text()
        count = re.search(r"verified_rows=(\d+)", output)
        if count is None or int(count.group(1)) != rows:
            raise RuntimeError(f"Offline checksum audit count differs: {output}")
        atomic_json(self.output / (label + ".json"),
                    {"status": "passed", "verified_rows": rows, "output": output,
                     "wall_ns": time.monotonic_ns() - started,
                     "space_before": before, "space_after": space_snapshot(data, stocks, times, strict=True)})

    def concurrent(self):
        data = self.args.large_data
        if data is None or not data.is_dir():
            raise ValueError("Concurrent large reads require the completed large dataset")
        stocks, times = (64, 480) if self.args.smoke else (20000, 241920)
        for workers in (0, 8):
            label = f"natural-concurrent-{workers}"
            self.start_server(data, label + "-server", cache=0, workers=workers)
            try:
                extra = ["--smoke"] if self.args.smoke else []
                self.checked_worker(self.command("benchmark_acceptance_concurrent.py", label,
                                                  data, stocks, times, extra), label, 10800)
            finally:
                self.stop_server()
        self.admin_check(data, "large", stocks * times, stocks, times)

    def archive(self):
        data = self.args.large_data
        if self.args.smoke:
            return
        if data is None or not data.is_relative_to(self.args.root / "data") or data.name != "l":
            raise ValueError("Only this acceptance run's generated large database may be archived")
        audit = json.loads((self.output / "admin-large.json").read_text())
        if audit["status"] != "passed" or audit["verified_rows"] != 4838400000:
            raise RuntimeError("Large dataset must pass the complete offline audit before archival")
        if self.args.archive_directory is None:
            raise ValueError("An independent archive directory is required")
        directory = self.args.archive_directory
        if directory == Path("/hd01/quotation") or Path("/hd01/quotation") in directory.parents:
            raise ValueError("The immutable quotation tree cannot contain test output")
        owned_directory(directory)
        archive = directory / "ohlc-large-4838400000-rows.tar.zst"
        command = ("tar --sparse -cf - -C " + shlex.quote(str(data.parent)) + " l | "
                   "zstd -T8 -3 --check -o " + shlex.quote(str(archive)))
        unit = self.launch("archive-large", ["bash", "-o", "pipefail", "-c", command], "16-23")
        self.wait(unit, "archive-large", timeout=21600)
        unit = self.launch("verify-large-archive", ["zstd", "--test", str(archive)], "16-23")
        self.wait(unit, "verify-large-archive", timeout=10800)
        # All source files are generated fixtures and the validated archive preserves them.
        record = {"archive": str(archive), "sha256": sha256(archive),
                  "archive_bytes": archive.stat().st_size, "dataset": str(data),
                  "validation": "tar pipeline exited successfully; zstd frame checksum verified",
                  "space_before_archival": audit["space_after"]}
        atomic_json(self.output / "large-archive.json", record)
        shutil.rmtree(data)
        self.results.append({"stage": "archive", "status": "passed", "archive": str(archive)})

    def recovery(self):
        stocks, times = (64, 480) if self.args.smoke else (10000, 15661)
        for fault in ("kill", "data", "current"):
            name = "recovery-" + fault
            data = self.clone(name)
            arm = self.output / (name + "-armed")
            environment = None if fault == "kill" else {
                "LD_PRELOAD": self.args.previous / "build/fault_inject.so",
                "OHLC_TEST_CRASH_ON": fault, "OHLC_TEST_CRASH_ARM": arm,
                "OHLC_TEST_DATABASE": data}
            server_label = name + "-fault-server"
            self.start_server(data, server_label, environment=environment)
            label = name + "-write"
            command = self.command("benchmark_acceptance_recovery.py", label, data, stocks, times,
                                   ["--fault", fault, "--duration", "2" if self.args.smoke else "60",
                                    "--arm", str(arm)])
            unit = self.launch(label, command, "24-39")
            ready_path = self.output / (label + "-ready.json")
            fault_started = None
            try:
                if fault == "kill":
                    deadline = time.monotonic() + 660
                    while not ready_path.is_file():
                        if time.monotonic() > deadline:
                            raise TimeoutError("Write stress did not reach the crash point")
                        if properties(unit).get("ActiveState") in ("inactive", "failed"):
                            raise RuntimeError("Write stress stopped before its crash marker")
                        time.sleep(.02)
                    fault_started = time.monotonic_ns()
                    subprocess.run(["systemctl", "kill", "--signal=SIGKILL", "--kill-whom=main",
                                    self.server_unit], check=True)
                self.wait(unit, label, timeout=660, server_required=False)
                server = properties(self.server_unit)
                expected_exit = "9" if fault == "kill" else "86"
                if server.get("ExecMainStatus") != expected_exit:
                    raise RuntimeError(f"Unexpected fault termination: {server}")
                trace = (self.output / (server_label + ".log")).read_text()
                if fault != "kill" and f"crash_boundary={fault} " not in trace:
                    raise RuntimeError("The intended sync boundary was not observed")
                ready = json.loads(ready_path.read_text())
                checkpoint_directory = self.output / (name + "-stopped-checkpoint")
                owned_directory(checkpoint_directory)
                snapshot = checkpoint_snapshot(data, checkpoint_directory)
                pending = ready["last_ack_sequence"] - snapshot["latest"]["sequence"]
                if fault == "kill" and pending <= 0:
                    raise RuntimeError("Crash was fully checkpointed; uncheckpointed recovery coverage not met")
                self.stop_server()
                recovery_ns = self.start_server(data, name + "-restarted")
                self.worker("verify", name + "-verify", data, stocks, ready["final_times"],
                            ["--verify-first", str(times)])
                self.stop_server()
                self.admin_check(data, name, stocks * ready["final_times"], stocks, ready["final_times"])
                result = {"stage": name, "status": "passed", "ready": ready,
                          "launch_to_listen_ns": recovery_ns, "crash_requested_ns": fault_started,
                          "acknowledged_sequences_after_checkpoint": pending,
                          "stopped_checkpoint": snapshot, "fault_service": server,
                          "fault_scope": "Process termination; host/device power loss is not simulated"}
                atomic_json(self.output / (name + "-result.json"), result)
                self.results.append(result)
            finally:
                self.stop(unit)
                self.stop_server()

    def mixed(self):
        data = self.clone("mixed")
        stocks, times = (64, 480) if self.args.smoke else (10000, 15661)
        self.start_server(data, "mixed-server")
        label = "mixed"
        command = self.command("benchmark_acceptance_mixed.py", label, data, stocks, times,
                               ["--duration", "20" if self.args.smoke else str(self.args.duration),
                                "--baseline", "3" if self.args.smoke else "60",
                                "--readers", "2" if self.args.smoke else "8",
                                "--correction-interval", "5" if self.args.smoke else "60"])
        try:
            self.checked_worker(command, label, self.args.duration + 14400)
        finally:
            self.stop_server()
        expected = json.loads((self.output / "expected-final.json").read_text())
        self.admin_check(data, "mixed", expected["rows"], stocks, expected["times"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--previous", type=Path, default=Path("/ssd02/ohlc-benchmark-20261110"))
    parser.add_argument("--label", required=True)
    parser.add_argument("--phases", nargs="+", choices=("concurrent", "archive", "recovery", "mixed"),
                        default=("recovery", "mixed"))
    parser.add_argument("--large-data", type=Path)
    parser.add_argument("--archive-directory", type=Path)
    parser.add_argument("--duration", type=float, default=86400)
    parser.add_argument("--wait-for-unit")
    parser.add_argument("--smoke", action="store_true")
    args = parser.parse_args()
    if os.geteuid() != 0 or not args.root.resolve().is_relative_to(Path("/ssd02")):
        parser.error("Run the controller as root with an isolated acceptance root under /ssd02")

    def interrupted(signum, frame):
        raise KeyboardInterrupt(f"Controller received signal {signum}")

    signal.signal(signal.SIGTERM, interrupted)
    if args.wait_for_unit:
        print(json.dumps({"event": "waiting_for_unit", "unit": args.wait_for_unit}), flush=True)
        while properties(args.wait_for_unit).get("ActiveState") not in ("inactive", "failed"):
            time.sleep(5)
    control = Acceptance(args)
    try:
        control.isolate()
        for phase in args.phases:
            try:
                getattr(control, phase)()
            except Exception as error:
                control.results.append({"stage": phase, "status": "failed", "error": repr(error),
                                        "traceback": traceback.format_exc()})
                control.stop_server()
                print(json.dumps(control.results[-1]), flush=True)
                if phase in ("concurrent", "archive") and "archive" in args.phases:
                    break
    finally:
        control.close()


if __name__ == "__main__":
    main()
