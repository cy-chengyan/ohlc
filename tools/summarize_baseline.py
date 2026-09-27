# SPDX-License-Identifier: Apache-2.0
"""Validate and summarize the raw edition-1 benchmark evidence without rounding inputs."""

import argparse
from array import array
import json
import math
from pathlib import Path
import statistics
import sys

BACKENDS = ("ohlc", "mysql", "clickhouse", "influxdb")


def read_lines(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def distribution(values):
    ordered = sorted(values)
    if not ordered:
        raise ValueError("No samples")
    return {"samples": len(ordered), "min_ns": ordered[0], "max_ns": ordered[-1],
            **{f"p{percent}_ns": ordered[math.ceil(len(ordered) * percent / 100) - 1]
               for percent in (50, 95, 99)}}


def raw_samples(path):
    values = array("Q")
    with path.open("rb") as source:
        size = path.stat().st_size
        if size % 8:
            raise ValueError(f"Truncated samples: {path}")
        values.fromfile(source, size // 8)
    if sys.byteorder != "little":
        values.byteswap()
    return values


def memory_summary(results, backend):
    own_path = results / f"{backend}-resources.jsonl"
    records = read_lines(own_path) if own_path.exists() else []
    observer = results / "resource-observer.jsonl"
    if observer.exists():
        records += [row for row in read_lines(observer) if row["backend"] == backend]
    if not records:
        raise ValueError(f"Missing resource observations for {backend}")
    resident = [row.get("rss_sum_bytes", row.get("VmRSS_bytes", 0)) for row in records]
    groups = [int(row["memory.current"]) for row in records if "memory.current" in row]
    swaps = [row.get("VmSwap_bytes", 0) for row in records]
    oom_kills = []
    for row in records:
        if "memory.events" in row:
            fields = dict(line.split() for line in row["memory.events"].splitlines())
            oom_kills.append(int(fields.get("oom_kill", 0)))
    return {"observations": len(records), "observed_rss_peak_bytes": max(resident),
            "main_process_hwm_bytes": max(row.get("VmHWM_bytes", 0) for row in records),
            "observed_cgroup_peak_bytes": max(groups, default=0),
            "observed_swap_bytes": max(swaps), "oom_kills": max(oom_kills, default=0)}


def summarize_backend(results, events, backend, expected, allow_influx_oom=False):
    related = [event for event in events if event.get("backend") == backend]
    completed = [event for event in related if event["event"] == "backend_completed"]
    validations = [event for event in related if event["event"] == "validated"]
    if len(completed) != 1 or len(validations) != 1:
        raise ValueError(f"Incomplete or ambiguous run: {backend}")
    if validations[0]["verified_rows"] != expected["rows"]:
        raise ValueError(f"Incomplete validation: {backend}")
    if "sums" in validations[0] and validations[0]["sums"] != expected["sums"]:
        raise ValueError(f"Aggregate mismatch: {backend}")
    loaded = read_lines(results / f"{backend}-load.jsonl")
    batches = [row for row in loaded if row["event"] == "batch"]
    if len(batches) != 858 or sum(row["rows"] for row in batches) != expected["rows"]:
        raise ValueError(f"Incomplete import: {backend}")
    for index, row in enumerate(batches):
        if (row["batch"] != index or row["rows"] != 100_000
                or row["total_rows"] != (index + 1) * 100_000):
            raise ValueError(f"Invalid import sequence: {backend}")
    if loaded[-1]["event"] != "loaded" or loaded[-1]["rows"] != expected["rows"]:
        raise ValueError(f"Missing import completion: {backend}")
    setup = next(row["wall_ns"] for row in loaded if row["event"] == "setup")
    settle = next(row["wall_ns"] for row in related if row["event"] == "settled")
    load = dict(loaded[-1], setup_ns=setup, settle_ns=settle,
                total_setup_load_settle_ns=setup + loaded[-1]["wall_ns"] + settle,
                rows_per_second=expected["rows"] * 1e9 / loaded[-1]["wall_ns"],
                batches=distribution([row["request_ns"] for row in batches]))

    warm_groups = {}
    warm_records = read_lines(results / f"{backend}-warm.jsonl")
    if len(warm_records) != 840:
        raise ValueError(f"Incomplete warmup or measured queries: {backend}")
    for row in warm_records:
        expected_rows = 10000 if row["kind"] == "X" else row["length"]
        if row["rows"] != expected_rows:
            raise ValueError(f"Incorrect query result size: {backend}")
        if row["measured"]:
            key = "cross" if row["kind"] == "X" else f"series{row['length']}"
            warm_groups.setdefault(key, []).append(row["wall_ns"])
    warm = {key: distribution(values) for key, values in warm_groups.items()}
    if len(warm) != 5 or any(item["samples"] != 120 for item in warm.values()):
        raise ValueError(f"Incomplete warm samples: {backend}")

    stress = {}
    for readers in (1, 4, 8, 16):
        rounds = []
        samples = {"series240": [], "cross": []}
        for repeat in range(3):
            directory = results / f"{backend}-stress-{readers}-{repeat}"
            path = directory / "summary.json"
            if not path.exists() or path.stat().st_size == 0:
                if allow_influx_oom and backend == "influxdb" and readers == 16 and repeat >= 1:
                    continue
                raise ValueError(f"Missing concurrency round: {directory}")
            record = json.loads(path.read_text())
            queries = sum(item["queries"] for item in record["kinds"])
            if record["readers"] != readers or record["wall_ns"] < 15_000_000_000:
                raise ValueError(f"Invalid concurrency interval: {directory}")
            rows = 0
            for kind in record["kinds"]:
                values = raw_samples(directory / (kind["kind"] + ".bin"))
                if len(values) != kind["queries"] or len(values) != kind["samples"]:
                    raise ValueError(f"Dropped latency samples: {directory}")
                measured = distribution(values)
                for percentile in (50, 95, 99):
                    key = f"p{percentile}_ns"
                    if measured[key] != kind[key]:
                        raise ValueError(f"Quantile mismatch: {directory}")
                samples[kind["kind"]].extend(values)
                rows += kind["queries"] * (240 if kind["kind"] == "series240" else 10000)
            rounds.append({"repeat": repeat, "queries": queries, "wall_ns": record["wall_ns"],
                           "qps": queries * 1e9 / record["wall_ns"],
                           "rows_per_second": rows * 1e9 / record["wall_ns"]})
        rates = [row["qps"] for row in rounds]
        passed = len(rounds) == 3
        if not passed and len(rounds) != 1:
            raise ValueError(f"Unexpected partial concurrency evidence: {backend} {readers}")
        stress[str(readers)] = {"rounds": rounds, "completed_rounds": len(rounds),
                                "status": "passed" if passed else "failed_oom",
                                "qps_median": statistics.median(rates) if passed else None,
                                "qps_min": min(rates) if passed else None,
                                "qps_max": max(rates) if passed else None,
                                **{key: distribution(values) for key, values in samples.items()}}

    cold_groups = {}
    for row in read_lines(results / f"{backend}-cold.jsonl"):
        key = "cross" if row["kind"] == "X" else f"series{row['length']}"
        cold_groups.setdefault(key, []).append(row)
    if len(cold_groups) != 3 or any(len(rows) != 5 for rows in cold_groups.values()):
        raise ValueError(f"Incomplete first-query samples: {backend}")
    cold = {}
    for kind, rows in cold_groups.items():
        values = [row["wall_ns"] for row in rows]
        cold[kind] = {"samples": len(values), "median_ns": statistics.median(values),
                      "min_ns": min(values), "max_ns": max(values),
                      "startup_median_ns": statistics.median(row["startup_ns"] for row in rows),
                      "max_remaining_resident_pages": max(row["eviction"]["resident_after"]
                                                          for row in rows)}
    storage = next(row for row in related if row["event"] == "storage")
    return {"load": load, "validation": validations[0], "warm": warm, "stress": stress,
            "cold": cold, "storage": storage, "memory": memory_summary(results, backend)}


def validate_query_targets(results):
    for phase in ("warm", "cold"):
        reference = None
        for backend in BACKENDS:
            records = read_lines(results / f"{backend}-{phase}.jsonl")
            targets = [(row["kind"], row["stock"], row["first"], row["length"])
                       for row in records]
            if reference is None:
                reference = targets
            elif targets != reference:
                raise ValueError(f"Query targets differ: {backend} {phase}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("evidence", type=Path, help="Directory containing main/results")
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    results = arguments.evidence / "main/results"
    events = read_lines(results / "events.jsonl")
    completions = [row for row in events if row["event"] == "completed"]
    if len(completions) != 1 or set(completions[0]["backends"]) != set(BACKENDS):
        raise ValueError("The measurement campaign did not complete")
    failures = [row for row in events if row["event"] == "failed"]
    declared = completions[0].get("failed_cases", [])
    allow_influx_oom = bool(failures)
    if allow_influx_oom:
        if (len(failures) != 1 or "influxdb-stress-16-1" not in failures[0]["error"]
                or len(declared) != 1 or declared[0]["backend"] != "influxdb"
                or declared[0]["readers"] != 16
                or memory_summary(results, "influxdb")["oom_kills"] < 1):
            raise ValueError("Unexpected failure; manual evidence review is required")
        recovered = [row for row in events if row["event"] == "recovery_validated"]
        if len(recovered) != 1 or recovered[0]["verified_rows"] != 85_800_000:
            raise ValueError("Missing recovery validation after the OOM")
    elif declared:
        raise ValueError("Declared failures have no matching failure event")
    expected = json.loads((results / "dataset.json").read_text())
    if expected["rows"] != 85_800_000:
        raise ValueError("This summary requires the full baseline dataset")
    summary = {"dataset": expected, "failed_cases": declared, "backends": {
        backend: summarize_backend(results, events, backend, expected, allow_influx_oom)
        for backend in BACKENDS}}
    validate_query_targets(results)
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    print(f"Verified and summarized all four backends: {arguments.output}")


if __name__ == "__main__":
    main()
