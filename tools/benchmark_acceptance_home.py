#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Control isolated home01 acceptance units; all database work runs as ohlc."""

import argparse
import json
import os
from pathlib import Path
import pwd
import shutil
import signal
import socket
import subprocess
import time
import traceback

from benchmark_home import properties
from benchmark_metrics import atomic_json, capture, proc_snapshot, sha256


def owned_directory(path):
    path.mkdir(parents=True, exist_ok=False)
    user = pwd.getpwnam("ohlc")
    os.chown(path, user.pw_uid, user.pw_gid)


class Controller:
    def __init__(self, args):
        self.args = args
        self.output = args.root / "results" / args.label
        owned_directory(self.output)
        self.data = args.root / "data" / args.label
        owned_directory(self.data)
        self.units = []
        self.counter = 0
        self.server_unit = None
        self.server_pid = None
        self.results = []
        user = pwd.getpwnam("ohlc")
        self.uid = user.pw_uid
        self.gid = user.pw_gid
        sources = self.output / "tools"
        owned_directory(sources)
        hashes = {}
        for path in (args.root / "source/tools").glob("benchmark_*.py"):
            shutil.copy2(path, sources / path.name)
            hashes[path.name] = sha256(path)
        atomic_json(self.output / "environment.json", {
            "product_provenance": json.loads((args.previous / "source/ohlc-benchmark-provenance.json").read_text()),
            "binaries": {name: sha256(args.previous / "build" / name) for name in
                         ("ohlcd", "ohlc-admin", "libohlc.so.0.1.0", "libohlc_client.so.0.1.0", "fault_inject.so")},
            "tools": hashes, "uid": self.uid, "server_cpus": "0-15", "client_cpus": "24-39",
            "arguments": {key: str(value) if isinstance(value, Path) else value
                          for key, value in vars(args).items()},
            "commands": [capture(command) for command in (["uname", "-a"], ["lscpu"],
                         ["free", "-b"], ["findmnt", "/ssd02"], ["df", "-B1", "/ssd02"])],
        })
        siblings = list(range(48, 64)) + list(range(72, 88))
        self.cpu_states = {f"/sys/devices/system/cpu/cpu{cpu}/online":
                           Path(f"/sys/devices/system/cpu/cpu{cpu}/online").read_text().strip()
                           for cpu in siblings}
        atomic_json(self.output / "smt-before.json", self.cpu_states)

    def isolate(self):
        os.sched_setaffinity(0, set(range(16, 24)))
        for path in self.cpu_states:
            Path(path).write_text("0")

    def launch(self, label, command, cpus, memory="32G", environment=None):
        self.counter += 1
        unit = f"ohlc-accept-{self.args.label}-{self.counter}-{label}"
        log = self.output / (label + ".log")
        log.touch(exist_ok=False)
        os.chown(log, self.uid, self.gid)
        options = ["systemd-run", "--quiet", "--unit=" + unit,
                   "--property=User=ohlc", "--property=Group=ohlc",
                   "--property=WorkingDirectory=" + str(self.args.root / "source"),
                   "--property=AllowedCPUs=" + cpus, "--property=CPUAffinity=" + cpus,
                   "--property=MemoryMax=" + memory, "--property=MemorySwapMax=0",
                   "--property=LimitNOFILE=65536", "--property=TimeoutStopSec=300",
                   "--property=ReadOnlyPaths=/hd01/quotation",
                   "--property=ReadOnlyPaths=" + str(self.args.previous / "seed"),
                   "--property=StandardOutput=append:" + str(log),
                   "--property=StandardError=append:" + str(log),
                   "--setenv=OPENBLAS_NUM_THREADS=1"]
        for key, value in (environment or {}).items():
            options.append("--setenv=" + key + "=" + str(value))
        atomic_json(self.output / (label + "-launch.json"),
                    {"unit": unit, "command": command, "options": options})
        subprocess.run(options + command, check=True)
        self.units.append(unit)
        print(json.dumps({"stage": label, "unit": unit, "event": "started"}), flush=True)
        return unit

    def wait(self, unit, label, timeout=10800, server_required=False):
        started = time.monotonic()
        with (self.output / (label + "-monitor.jsonl")).open("x", buffering=1) as stream:
            while True:
                current = properties(unit)
                server = properties(self.server_unit) if self.server_unit else {}
                record = {"time": time.time(), "worker": current, "server": server,
                          "process": proc_snapshot(int(current.get("MainPID", "0"))),
                          "server_process": proc_snapshot(self.server_pid) if self.server_pid else {},
                          "vmstat": Path("/proc/vmstat").read_text(),
                          "diskstats": Path("/proc/diskstats").read_text(),
                          "loadavg": Path("/proc/loadavg").read_text().strip()}
                stream.write(json.dumps(record) + "\n")
                if current.get("ActiveState") in ("inactive", "failed"):
                    if current.get("Result") != "success" or current.get("ExecMainStatus") != "0":
                        raise RuntimeError(f"Worker failed: {current}")
                    return record
                if server_required and server.get("ActiveState") != "active":
                    raise RuntimeError(f"Server stopped unexpectedly: {server}")
                if any(int(item.get("memory.swap.current", "0")) for item in (current, server)):
                    raise RuntimeError("Acceptance cgroup swapped")
                free = os.statvfs(self.args.root)
                if free.f_bavail * free.f_frsize < 64 << 30:
                    raise RuntimeError("Filesystem reserve fell below 64 GiB")
                if time.monotonic() - started > timeout:
                    raise TimeoutError(f"Acceptance stage exceeded {timeout} seconds")
                time.sleep(2)

    def stop(self, unit):
        if unit is not None:
            state = properties(unit)
            if state.get("ActiveState") not in ("inactive", "failed"):
                subprocess.run(["systemctl", "stop", unit], check=True)

    def start_server(self, data, label, cache=64, memory=24576, workers=0, environment=None):
        try:
            with socket.create_connection(("127.0.0.1", 18766), timeout=.2):
                raise RuntimeError("Acceptance port 18766 already in use")
        except ConnectionRefusedError:
            pass
        config = self.output / (label + ".conf")
        config.write_text(f"data={data}\nhost=127.0.0.1\nport=18766\n"
                          f"memory-mib={memory}\ncache-mib={cache}\nnetwork-mib=1024\n"
                          "connections=128\nquery-ms=300000\ntimeout-ms=300000\n"
                          f"busy-wait-ms=30000\nread-workers={workers}\n")
        command = [str(self.args.previous / "build/ohlcd"), "--config", str(config)]
        started = time.monotonic_ns()
        self.server_unit = self.launch(label, command, "0-15", environment=environment)
        deadline = time.monotonic() + 1800
        while time.monotonic() < deadline:
            info = properties(self.server_unit)
            if info.get("ActiveState") in ("inactive", "failed"):
                raise RuntimeError(f"Server startup failed: {info}")
            try:
                with socket.create_connection(("127.0.0.1", 18766), timeout=.2):
                    self.server_pid = int(info["MainPID"])
                    elapsed = time.monotonic_ns() - started
                    atomic_json(self.output / (label + "-ready.json"),
                                {"launch_to_listen_ns": elapsed, "server": info})
                    return elapsed
            except (ConnectionRefusedError, TimeoutError):
                time.sleep(.02)
        raise TimeoutError("Server recovery did not finish within 30 minutes")

    def stop_server(self):
        self.stop(self.server_unit)
        self.server_unit = None
        self.server_pid = None

    def worker(self, action, label, data, stocks, times, extra=None, timeout=10800):
        command = [str(self.args.previous / "venv/bin/python"),
                   "tools/benchmark_acceptance.py", action,
                   "--seed", str(self.args.previous / "seed"),
                   "--binary", str(self.args.previous / "build/ohlcd"),
                   "--data", str(data), "--output", str(self.output / (label + ".jsonl")),
                   "--stocks", str(stocks), "--times", str(times)] + (extra or [])
        if self.args.smoke:
            command.append("--smoke")
        embedded = "embedded" in command
        unit = self.launch(label, command, "0-15" if embedded else "24-39")
        try:
            result = self.wait(unit, label, timeout, server_required=not embedded)
            last = json.loads((self.output / (label + ".jsonl")).read_text().splitlines()[-1])
            if last.get("event") != "complete" or last.get("status") != "passed":
                raise RuntimeError(f"Worker did not complete: {last}")
            self.results.append({"stage": label, "status": "passed", "final": result})
            atomic_json(self.output / "progress.json", self.results)
        finally:
            self.stop(unit)

    def copy_baseline(self):
        data = self.data / "c"
        owned_directory(data)
        source = self.args.previous / "results/c01/ohlc-data"
        unit = self.launch("copy-baseline", ["cp", "-a", "--reflink=auto", str(source) + "/.",
                                           str(data)], "16-23")
        self.wait(unit, "copy-baseline", timeout=600)
        return data

    def cold(self):
        if self.args.smoke:
            data = self.data / "c"
            owned_directory(data)
            self.start_server(data, "smoke-server")
            self.worker("import", "smoke-import", data, 64, 480)
            self.stop_server()
            stocks, times = 64, 480
        else:
            data = self.copy_baseline()
            stocks, times = 10000, 15661
        for state, cache in (("hot", 0), ("hot", 64), ("cold", 0)):
            self.worker("read", f"{state}-{cache}", data, stocks, times,
                        ["--mode", "embedded", "--cache-state", state, "--cache-mib", str(cache)])

    def large(self):
        data = self.data / "l"
        owned_directory(data)
        stocks, times = (64, 480) if self.args.smoke else (20000, 241920)
        self.start_server(data, "large-import-server")
        self.worker("import", "large-import", data, stocks, times, timeout=10800)
        self.stop_server()
        for workers in (0, 8):
            self.worker("read", f"large-natural-{workers}", data, stocks, times,
                        ["--mode", "embedded", "--cache-state", "natural", "--cache-mib", "0",
                         "--read-workers", str(workers)])

    def close(self):
        errors = []
        for unit in reversed(self.units):
            try:
                self.stop(unit)
            except Exception as error:
                errors.append({"unit": unit, "error": repr(error)})
        for path, state in self.cpu_states.items():
            Path(path).write_text(state)
        atomic_json(self.output / "smt-after.json", {path: Path(path).read_text().strip()
                                                    for path in self.cpu_states})
        atomic_json(self.output / "summary.json", {"stages": self.results, "cleanup_errors": errors})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--previous", type=Path, default=Path("/ssd02/ohlc-benchmark-20261110"))
    parser.add_argument("--label", required=True)
    parser.add_argument("--phases", nargs="+", choices=("cold", "large"), default=("cold", "large"))
    parser.add_argument("--smoke", action="store_true")
    args = parser.parse_args()
    if os.geteuid() != 0:
        parser.error("The controller requires root; workers always run as ohlc")
    if not args.root.resolve().is_relative_to(Path("/ssd02")):
        parser.error("Use an isolated acceptance root under /ssd02")

    def interrupted(signum, frame):
        raise KeyboardInterrupt(f"Controller received signal {signum}")

    signal.signal(signal.SIGTERM, interrupted)
    control = Controller(args)
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
    finally:
        control.close()


if __name__ == "__main__":
    main()
