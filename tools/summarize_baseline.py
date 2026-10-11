# SPDX-License-Identifier: Apache-2.0
"""Validate and summarize the raw four-database benchmark evidence without rounding inputs."""

import argparse
from array import array
import json
import math
from pathlib import Path
import statistics
import sys

BACKENDS = ("ohlc", "clickhouse", "influxdb", "aerospike")


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
            "kernel_cgroup_peak_bytes": max((int(row["memory.peak"]) for row in records
                                              if "memory.peak" in row), default=0),
            "observed_swap_bytes": max(swaps), "oom_kills": max(oom_kills, default=0)}


def summarize_backend(results, events, backend, expected, *, allow_incomplete=False):
    related = [event for event in events if event.get("backend") == backend]
    completed = [event for event in related if event["event"] == "backend_completed"]
    validations = [event for event in related if event["event"] == "validated"]
    if len(completed) != (0 if allow_incomplete else 1) or len(validations) != 1:
        raise ValueError(f"Incomplete or ambiguous run: {backend}")
    if validations[0]["verified_rows"] != expected["rows"]:
        raise ValueError(f"Incomplete validation: {backend}")
    if "sums" in validations[0] and validations[0]["sums"] != expected["sums"]:
        raise ValueError(f"Aggregate mismatch: {backend}")
    loaded = read_lines(results / f"{backend}-load.jsonl")
    batches = [row for row in loaded if row["event"] == "batch"]
    if len(batches) != 500 or sum(row["rows"] for row in batches) != expected["rows"]:
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
            declared = [row for row in related if row["event"] == "stress_completed"
                        and row["threads"] == readers and row["round"] == repeat]
            if not declared and allow_incomplete:
                break
            if len(declared) != 1:
                raise ValueError(f"Missing or ambiguous concurrency event: {directory}")
            path = directory / "summary.json"
            if not path.exists() or path.stat().st_size == 0:
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
        if not passed:
            if not allow_incomplete:
                raise ValueError(f"Incomplete concurrency evidence: {backend} {readers}")
            stress[str(readers)] = {"rounds": rounds, "completed_rounds": len(rounds),
                                    "status": "not_completed"}
            continue
        stress[str(readers)] = {"rounds": rounds, "completed_rounds": len(rounds),
                                "status": "passed",
                                "qps_median": statistics.median(rates),
                                "qps_min": min(rates),
                                "qps_max": max(rates),
                                **{key: distribution(values) for key, values in samples.items()}}

    cold_groups = {}
    cold_completed = any(row["event"] == "cold_completed" for row in related)
    cold_expected = {"series240": 3, "series5000": 3, "cross": 3}
    adjustments = [row for row in related if row["event"] == "protocol_adjusted"]
    if adjustments:
        if (len(adjustments) != 1 or backend != "aerospike"
                or adjustments[0]["cold_samples"] != 2
                or adjustments[0]["cold_plan"] != [{"kind": "S", "length": 240, "samples": 2}]):
            raise ValueError(f"Unexpected protocol adjustment: {backend}")
        cold_expected = {"series240": 2}
    if cold_completed or not allow_incomplete:
        for row in read_lines(results / f"{backend}-cold.jsonl"):
            key = "cross" if row["kind"] == "X" else f"series{row['length']}"
            cold_groups.setdefault(key, []).append(row)
        if {kind: len(rows) for kind, rows in cold_groups.items()} != cold_expected:
            raise ValueError(f"Incomplete first-query samples: {backend}")
    cold = {}
    for kind, rows in cold_groups.items():
        values = [row["wall_ns"] for row in rows]
        cold[kind] = {"samples": len(values), "median_ns": statistics.median(values),
                      "min_ns": min(values), "max_ns": max(values),
                      "startup_median_ns": statistics.median(row["startup_ns"] for row in rows),
                      "max_remaining_resident_pages": max(row["eviction"]["resident_after"]
                                                          for row in rows)}
    storage = next((row for row in related if row["event"] == "storage"), None)
    if storage is None and not allow_incomplete:
        raise ValueError(f"Missing storage inventory: {backend}")
    return {"load": load, "validation": validations[0], "warm": warm, "stress": stress,
            "cold": cold, "storage": storage, "memory": memory_summary(results, backend)}


def validate_query_targets(results):
    for phase in ("warm", "cold"):
        reference = None
        for backend in BACKENDS:
            path = results / f"{backend}-{phase}.jsonl"
            if not path.exists():
                continue
            records = read_lines(path)
            lengths = (840,) if phase == "warm" else ((2, 9) if backend == "aerospike" else (9,))
            if len(records) not in lengths:
                continue
            targets = [(row["kind"], row["stock"], row["first"], row["length"])
                       for row in records]
            if reference is None:
                reference = targets
            elif targets != reference[:len(targets)]:
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
    failures = [row for row in events if row["event"] == "backend_failed"]
    declared = completions[0].get("failed_cases", [])
    if {(row["backend"], row["stage"], row["error"]) for row in failures} != {
            (row["backend"], row["stage"], row["error"]) for row in declared}:
        raise ValueError("Failure declarations do not match recorded events")
    expected = json.loads((results / "dataset.json").read_text())
    if expected["rows"] != 50_000_000:
        raise ValueError("This summary requires the full baseline dataset")
    failed = {row["backend"]: row for row in declared}
    summary = {"dataset": expected, "failed_cases": declared, "backends": {}}
    for backend in BACKENDS:
        limits = json.loads((results / f"{backend}-limits.json").read_text())
        if (limits["memory.max"] != str(32 << 30) or limits["memory.swap.max"] != "0"
                or limits["cpuset.cpus.effective"] != "0-15"):
            raise ValueError(f"Incorrect service limits: {backend}")
        if backend in failed:
            related = [row for row in events if row.get("backend") == backend]
            measured = {"status": "failed", "failure": failed[backend],
                        "completed_stages": [row["event"] for row in related],
                        "memory": memory_summary(results, backend)}
            if "warm_completed" in measured["completed_stages"]:
                measured.update(summarize_backend(results, events, backend, expected,
                                                   allow_incomplete=True))
            inventory = results / f"{backend}-storage-after-failure.json"
            if inventory.exists():
                storage = json.loads(inventory.read_text())
                files = json.loads((results / f"{backend}-files-after-failure.json").read_text())
                if (storage["event"] != "storage_after_failure"
                        or storage["logical_bytes"] != sum(row["logical"] for row in files)
                        or storage["allocated_bytes"] != sum(row["allocated"] for row in files)):
                    raise ValueError(f"Invalid post-failure storage inventory: {backend}")
                measured["storage"] = storage
        else:
            measured = summarize_backend(results, events, backend, expected)
            measured["status"] = "passed"
        measured["limits"] = limits
        summary["backends"][backend] = measured
    validate_query_targets(results)
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    print(f"Validated four-backend results and failure declarations: {arguments.output}")


if __name__ == "__main__":
    main()
