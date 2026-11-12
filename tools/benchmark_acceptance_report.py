#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Summarize acceptance evidence without treating unfinished work as a passing result."""

import argparse
from collections import Counter
import json
from pathlib import Path
import time

from benchmark_metrics import atomic_json


def events(path):
    """A live file may end with one incomplete record; malformed complete records are errors."""
    with path.open() as stream:
        for line in stream:
            if not line.endswith("\n"):
                return
            yield json.loads(line)


def counter_text(value):
    result = {}
    for line in value.splitlines():
        parts = line.split()
        if len(parts) == 2:
            result[parts[0]] = int(parts[1])
    return result


def monitor_summary(path):
    result = {"samples": 0, "worker": {}, "server": {}}
    for event in events(path):
        result["samples"] += 1
        result.setdefault("first_time", event["time"])
        result["last_time"] = event["time"]
        for role in ("worker", "server"):
            values = event.get(role, {})
            summary = result[role]
            for name in ("memory.current", "memory.peak", "memory.swap.current"):
                if name in values:
                    summary[name + "_maximum"] = max(summary.get(name + "_maximum", 0),
                                                     int(values[name]))
            for name, count in counter_text(values.get("memory.events", "")).items():
                key = "memory.events." + name
                summary[key] = max(summary.get(key, 0), count)
            memory = counter_text(values.get("memory.stat", ""))
            for name in ("anon", "file", "shmem", "file_dirty", "file_writeback"):
                if name in memory:
                    key = "memory.stat." + name + "_maximum"
                    summary[key] = max(summary.get(key, 0), memory[name])
            summary["final_status"] = {key: values[key] for key in
                                       ("ActiveState", "Result", "ExecMainStatus") if key in values}
            process = event.get("process" if role == "worker" else "server_process", {})
            for key in ("peak_rss_bytes", "rss_bytes", "swap_bytes"):
                if key in process:
                    summary[key + "_maximum"] = max(summary.get(key + "_maximum", 0), process[key])
            if "cpu_ns" in process:
                summary.setdefault("first_process", process)
                summary["last_process"] = process
    return result


def workload_summary(path):
    retained = ("workload", "open", "cache_preparation", "over_memory", "import_complete",
                "import_probes_passed", "latency_summary", "throughput_summary",
                "verification_complete", "baseline_cache_residency", "mixed_complete",
                "audit_complete", "ready_for_crash", "checkpoint_disconnected", "failed")
    result = {"events": {}, "measurements": [], "states": [], "status": "incomplete"}
    counts = Counter()
    lag = []
    last_progress = None
    last_event = None
    for event in events(path):
        name = event["event"]
        counts[name] += 1
        result.setdefault("first_time", event["time"])
        result["last_time"] = event["time"]
        last_event = event
        if name in retained:
            result["measurements"].append(event)
        elif name == "state":
            result["states"].append(event)
        elif name == "writer_lag":
            lag.append(event["behind_seconds"])
        elif name in ("mixed_progress", "import_batch", "audit_progress"):
            last_progress = event
    result["events"] = dict(counts)
    result["last_progress"] = last_progress
    result["last_event"] = last_event
    if last_event and last_event["event"] == "complete":
        result["status"] = last_event["status"]
    elif counts["failed"]:
        result["status"] = "failed"
    if lag:
        result["writer_lag"] = {"events": len(lag), "maximum_seconds": max(lag),
                                "sum_seconds": sum(lag)}
    return result


def reader_summary(path):
    result = {"status": "incomplete", "phases": {}, "windows": []}
    for event in events(path):
        if event["event"] == "read_window":
            result["windows"].append(event)
            phase = result["phases"].setdefault(str(event["phase"]), {
                "requests": 0, "duration_ns": 0, "verified_queries": 0, "verify_ns": 0})
            for key in ("requests", "duration_ns", "verified_queries", "verify_ns"):
                phase[key] += event[key]
            phase["maximum_latency_ns"] = max(phase.get("maximum_latency_ns", 0),
                                                event["latency_ns"]["max"])
            phase.setdefault("kind", event["kind"])
        elif event["event"] in ("complete", "failed"):
            result["final_event"] = event
            result["status"] = event.get("status", "failed")
    for phase in result["phases"].values():
        phase["qps"] = phase["requests"] * 1e9 / phase["duration_ns"]
        phase["verification_wall_fraction"] = phase["verify_ns"] / phase["duration_ns"]
    return result


def collect(root, include_monitors):
    result = {"recorded_at": time.time(), "root": str(root), "runs": {},
              "interpretation": "Measured values are evidence, not a blanket acceptance verdict. "
                                "An absent completion event is always incomplete."}
    for directory in sorted((root / "results").iterdir()):
        if not directory.is_dir() or not (directory / "environment.json").is_file():
            continue
        run = {"environment": json.loads((directory / "environment.json").read_text()),
               "workloads": {}, "readers": {}, "audits": {}, "recovery": {}}
        result["runs"][directory.name] = run
        summary = directory / "summary.json"
        if summary.is_file():
            run["controller"] = json.loads(summary.read_text())
        for path in sorted(directory.glob("*.jsonl")):
            if path.name.endswith("-monitor.jsonl"):
                if include_monitors:
                    run.setdefault("monitors", {})[path.stem] = monitor_summary(path)
            elif path.name.startswith("reader-"):
                run["readers"][path.stem] = reader_summary(path)
            elif not path.name.endswith("-ledger.jsonl"):
                run["workloads"][path.stem] = workload_summary(path)
        for path in sorted(directory.glob("admin-*.json")):
            if not path.name.endswith("-launch.json"):
                run["audits"][path.stem] = json.loads(path.read_text())
        for path in sorted(directory.glob("recovery-*-result.json")):
            run["recovery"][path.stem] = json.loads(path.read_text())
        for name in ("large-archive.json", "expected-final.json", "smt-before.json", "smt-after.json"):
            path = directory / name
            if path.is_file():
                run[path.stem] = json.loads(path.read_text())
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--include-monitors", action="store_true",
                        help="Parse full monitor histories after measurements finish")
    args = parser.parse_args()
    atomic_json(args.output, collect(args.root, args.include_monitors))
    print(json.dumps({"output": str(args.output), "bytes": args.output.stat().st_size}))


if __name__ == "__main__":
    main()
