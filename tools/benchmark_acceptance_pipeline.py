#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Continue the existing home01 run after validated prerequisites and preserve final evidence."""

import argparse
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import time
import traceback

from benchmark_home import properties
from benchmark_metrics import atomic_json, sha256


def wait_unit(unit, timeout):
    deadline = time.monotonic() + timeout
    while True:
        state = properties(unit)
        if state.get("ActiveState") in ("inactive", "failed"):
            return state
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Controller still running after {timeout} seconds: {unit}")
        time.sleep(10)


def require_run(root, label, required):
    directory = root / "results" / label
    summary = json.loads((directory / "summary.json").read_text())
    if summary["cleanup_errors"]:
        raise RuntimeError(f"Cleanup failed in {label}: {summary['cleanup_errors']}")
    stages = {entry["stage"]: entry["status"] for entry in summary["stages"]}
    if any(status == "failed" for status in stages.values()):
        raise RuntimeError(f"Prerequisite run failed: {label}: {stages}")
    for stage in required:
        if stages.get(stage) not in ("passed", "unstable"):
            raise RuntimeError(f"Required evidence absent: {label}/{stage}: {stages}")
    if json.loads((directory / "smt-before.json").read_text()) != json.loads(
            (directory / "smt-after.json").read_text()):
        raise RuntimeError(f"SMT state was not restored by {label}")


def finalize(args, error):
    root = args.root
    log = root / "results/source-after.log"
    unit = "ohlc-acceptance-source-after-20261111"
    command = ["systemd-run", "--quiet", "--unit=" + unit,
               "--property=User=ohlc", "--property=Group=ohlc",
               "--property=WorkingDirectory=" + str(root / "source"),
               "--property=AllowedCPUs=16-23", "--property=CPUAffinity=16-23",
               "--property=MemoryMax=32G", "--property=MemorySwapMax=0",
               "--property=ReadOnlyPaths=/hd01/quotation",
               "--property=ReadOnlyPaths=" + str(args.previous / "seed"),
               "--property=StandardOutput=append:" + str(log),
               "--property=StandardError=append:" + str(log),
               "--setenv=OPENBLAS_NUM_THREADS=1", str(args.previous / "venv/bin/python"),
               "tools/benchmark_verify.py", str(args.previous / "seed"), "--source"]
    subprocess.run(command, check=True)
    verification_state = wait_unit(unit, 1800)
    verified = None
    if verification_state.get("Result") == "success" and verification_state.get("ExecMainStatus") == "0":
        verified = json.loads(log.read_text().splitlines()[-1])
        atomic_json(root / "results/source-after.json", verified)
    smt = {}
    for path in sorted((root / "results").glob("*/smt-before.json")):
        after = path.with_name("smt-after.json")
        smt[path.parent.name] = after.is_file() and json.loads(path.read_text()) == json.loads(after.read_text())
    open_ports = []
    for port in (18765, 18766, 18123, 18086, 13000, 13001, 13002, 13003):
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=.2):
                open_ports.append(port)
        except (ConnectionRefusedError, TimeoutError):
            pass
    record = {"time": time.time(), "pipeline_error": error, "source_verification": verified,
              "source_verification_service": verification_state, "smt_restored": smt,
              "open_test_ports": open_ports,
              "cpu_online": Path("/sys/devices/system/cpu/online").read_text().strip()}
    record["cleanup_passed"] = (verified is not None and verified.get("status") == "passed" and
                                verified.get("source_files_verified") == 1023 and
                                all(smt.values()) and not open_ports)
    atomic_json(root / "results/finalization.json", record)
    subprocess.run(["python3.12", str(root / "source/tools/benchmark_acceptance_report.py"),
                    str(root), "--output", str(root / "results/final-summary.json"),
                    "--include-monitors"], cwd=root / "source", check=True)
    evidence = {}
    for path in sorted((root / "results").rglob("*")):
        if (not path.is_file() or path.name in ("evidence-sha256.json", "pipeline-state.json",
                                                "pipeline-controller.log", "stage-summary.json")):
            continue
        evidence[str(path.relative_to(root))] = {"bytes": path.stat().st_size, "sha256": sha256(path)}
    atomic_json(root / "results/evidence-sha256.json", evidence)
    archive = root / "evidence.tar.gz"
    subprocess.run(["tar", "-czf", str(archive), "--exclude=results/pipeline-controller.log",
                    "--exclude=results/pipeline-state.json", "--exclude=results/stage-summary.json",
                    "results"], cwd=root, check=True)
    atomic_json(root / "evidence-archive.json", {"path": str(archive), "bytes": archive.stat().st_size,
                                                "sha256": sha256(archive)})
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--previous", type=Path, default=Path("/ssd02/ohlc-benchmark-20261110"))
    parser.add_argument("--duration", type=float, default=86400)
    args = parser.parse_args()
    if os.geteuid() != 0 or args.root != Path("/ssd02/ohlc-acceptance-20261111"):
        parser.error("This supervisor is restricted to the authorized home01 acceptance root")
    os.sched_setaffinity(0, set(range(16, 24)))
    state_path = args.root / "results/pipeline-state.json"

    def state(stage, **extra):
        value = {"time": time.time(), "stage": stage, "duration_seconds": args.duration, **extra}
        atomic_json(state_path, value)
        print(json.dumps(value), flush=True)

    error = None
    try:
        state("waiting_for_large_and_tool_smoke")
        wait_unit("ohlc-acceptance-large01-20261111", 21600)
        wait_unit("ohlc-acceptance-postsmoke01-20261111", 7200)
        require_run(args.root, "large01", ("large-import", "large-natural-0", "large-natural-8"))
        require_run(args.root, "postsmoke01", ("natural-concurrent-0", "natural-concurrent-8",
                                              "recovery-kill", "recovery-data", "recovery-current", "mixed"))
        for name in ("large", "recovery-kill", "recovery-data", "recovery-current", "mixed"):
            audit = json.loads((args.root / "results/postsmoke01" / f"admin-{name}.json").read_text())
            if audit["status"] != "passed":
                raise RuntimeError(f"Tool smoke audit failed: {name}")
        pending = args.root / "pending-tools"
        manifest = json.loads((pending / "sha256.json").read_text())
        for name, digest in manifest.items():
            source = pending / name
            if source.name != name or sha256(source) != digest:
                raise RuntimeError(f"Pending tool fingerprint differs: {name}")
        for name in manifest:
            shutil.copy2(pending / name, args.root / "source/tools" / name)
        state("starting_full_followup", promoted_tools=manifest)
        unit = "ohlc-acceptance-post01-20261111"
        log = args.root / "results/post01-controller.log"
        command = ["systemd-run", "--quiet", "--unit=" + unit,
                   "--property=AllowedCPUs=16-23", "--property=CPUAffinity=16-23",
                   "--property=StandardOutput=append:" + str(log),
                   "--property=StandardError=append:" + str(log),
                   "--setenv=OPENBLAS_NUM_THREADS=1", str(args.previous / "venv/bin/python"),
                   str(args.root / "source/tools/benchmark_acceptance_continue.py"), str(args.root),
                   "--label", "post01", "--phases", "concurrent", "archive", "recovery", "mixed",
                   "--large-data", str(args.root / "data/large01/l"),
                   "--archive-directory", "/hd01/ohlc-acceptance-20261111-large01",
                   "--duration", str(args.duration)]
        atomic_json(args.root / "results/post01-controller-launch.json", {"command": command})
        subprocess.run(command, check=True)
        state("full_followup_running", unit=unit)
        wait_unit(unit, args.duration + 43200)
        require_run(args.root, "post01", ("natural-concurrent-0", "natural-concurrent-8", "archive",
                                          "recovery-kill", "recovery-data", "recovery-current", "mixed"))
        expected = json.loads((args.root / "results/post01/expected-final.json").read_text())
        audit = json.loads((args.root / "results/post01/admin-mixed.json").read_text())
        if audit["status"] != "passed" or audit["verified_rows"] != expected["rows"]:
            raise RuntimeError("The sustained run has no matching final offline audit")
    except Exception as failure:
        error = {"error": repr(failure), "traceback": traceback.format_exc()}
        state("execution_failed", **error)
    finally:
        active = [unit for unit in ("ohlc-acceptance-large01-20261111",
                                    "ohlc-acceptance-postsmoke01-20261111",
                                    "ohlc-acceptance-post01-20261111")
                  if properties(unit).get("ActiveState") not in ("inactive", "failed")]
        if active:
            state("finalization_deferred", active_controllers=active, error=error)
        else:
            state("finalizing", error=error)
            try:
                final = finalize(args, error)
                state("finished", execution_error=error, cleanup_passed=final["cleanup_passed"])
            except Exception as failure:
                state("finalization_failed", error=repr(failure), traceback=traceback.format_exc())
                raise


if __name__ == "__main__":
    main()
