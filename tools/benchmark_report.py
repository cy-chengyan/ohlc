#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Summarize raw comparison evidence without hiding retries or incomplete groups."""

import argparse
import json
from pathlib import Path
import statistics

from benchmark_metrics import atomic_json, summarize


def records(path):
    with path.open() as stream:
        for line in stream:
            yield json.loads(line)


def vmstat(raw):
    return {key: int(value) for key, value in map(str.split, raw.splitlines())}


def system_report(root, name):
    result = {"system": name, "latency": {}, "throughput": [], "errors": []}
    raw = root / (name + ".jsonl")
    if not raw.exists():
        result["status"] = "not_run"
        return result
    rounds = {}
    for item in records(raw):
        event = item["event"]
        if event in ("import_model", "import_complete", "realtime_summary",
                     "space_after_import", "space_after_realtime", "recovery_validation",
                     "complete"):
            result[event] = item
        elif event == "latency_summary":
            label = {0: "X", 15360: "RFULL"}.get(item["length"], "R" + str(item["length"]))
            result["latency"][label] = item["wall_ns"]
        elif event == "throughput_round" and not item["warmup"]:
            key = (item["kind"], item["readers"], item["attempt"])
            rounds.setdefault(key, []).append(item)
        elif event == "throughput_summary":
            key = (item["kind"], item["readers"], item["attempt"])
            measurements = rounds.pop(key)
            qps = [entry["qps"] for entry in measurements]
            latency = [value for entry in measurements for reader in entry["latency_ns"]
                       for value in reader]
            result["throughput"].append({
                **item, "qps": qps, "mean_qps": statistics.mean(qps),
                "latency_ns": summarize(latency),
                "verify_fraction": statistics.mean(entry["verify_fraction"] for entry in measurements),
                "prepare_fraction": statistics.mean(entry["prepare_fraction"] for entry in measurements),
                "oracle_wait_fraction": statistics.mean(entry["oracle_wait_fraction"] for entry in measurements),
                "fully_verified_queries": sum(entry["full_verify_queries"] for entry in measurements),
            })
        elif event == "invalid":
            result["errors"].append(item)
        elif event == "complete":
            result["status"] = item["validity"]
    result.setdefault("status", "incomplete")
    result["incomplete_throughput_groups"] = [
        {"kind": kind, "readers": readers, "attempt": attempt, "completed_rounds": len(items)}
        for (kind, readers, attempt), items in rounds.items()
    ]
    completed = root / (name + "-result.json")
    if completed.exists():
        controller = json.loads(completed.read_text())
        result["status"] = controller["status"]
        result["controller_result"] = controller
    monitor = root / (name + "-monitor.jsonl")
    peak_memory = 0
    peak_rss = 0
    swapped = 0
    memory_events = {}
    first_swap = None
    last_swap = None
    if monitor.exists():
        for item in records(monitor):
            server = item["server"]
            if not server.get("ControlGroup"):
                continue
            for key in ("memory.peak", "MemoryPeak", "memory.current"):
                if str(server.get(key, "")).isdigit() and int(server[key]) < 1 << 63:
                    peak_memory = max(peak_memory, int(server[key]))
            peak_rss = max(peak_rss, item.get("server_process", {}).get("peak_rss_bytes", 0))
            swapped = max(swapped, int(server.get("memory.swap.current", "0")))
            for key, value in vmstat(server.get("memory.events", "")).items():
                memory_events[key] = max(memory_events.get(key, 0), value)
            current = vmstat(item["vmstat"])
            values = [current.get("pswpin", 0), current.get("pswpout", 0)]
            if first_swap is None:
                first_swap = values
            last_swap = values
    result["resources"] = {"server_cgroup_peak_bytes": peak_memory, "server_peak_rss_bytes": peak_rss,
                           "server_swap_max_observed_bytes": swapped,
                           "server_memory_events": memory_events,
                           "host_swap_pages_delta": [a - b for a, b in zip(last_swap or [], first_swap or [])]}
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    result = {name: system_report(args.directory, name)
              for name in ("ohlc", "clickhouse", "influxdb", "aerospike")}
    atomic_json(args.directory / "report.json", result)
    for name, item in result.items():
        print(name, item["status"],
              "import rows/s", item.get("import_complete", {}).get("rows_per_second"),
              "R240 ms", item["latency"].get("R240", {}).get("p50", 0) / 1e6,
              "X ms", item["latency"].get("X", {}).get("p50", 0) / 1e6)
