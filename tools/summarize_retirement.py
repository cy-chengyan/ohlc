# SPDX-License-Identifier: Apache-2.0
"""Validate and summarize the lifecycle benchmark's raw measurements."""

import argparse
import json
import math
from pathlib import Path
import statistics


LABELS = ("cross_recent", "cross_history", "series240", "series2048")


def read_lines(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def distribution(values):
    ordered = sorted(values)
    if not ordered:
        raise ValueError("Missing samples")
    return {"samples": len(ordered), "minimum": ordered[0], "maximum": ordered[-1],
            "median": statistics.median(ordered),
            **{f"p{percent}": ordered[math.ceil(len(ordered) * percent / 100) - 1]
               for percent in (50, 95, 99)}}


def only_event(events, name, **conditions):
    found = [row for row in events if row["event"] == name
             and all(row.get(key) == value for key, value in conditions.items())]
    if len(found) != 1:
        raise ValueError(f"Expected one event: {name} {conditions}")
    return found[0]


def summarize_case(directory):
    events = read_lines(directory / "events.jsonl")
    if any(row["event"] == "failed" for row in events):
        raise ValueError(f"Failed case: {directory}")
    completed = only_event(events, "case_completed")
    only_event(events, "seed_copy_verified")
    only_event(events, "temporary_data_removed")
    configuration = json.loads((directory / "fixture.json").read_text())
    layout = json.loads((directory / "model.json").read_text())
    lives = [tuple(map(int, line.split())) for line in
             (directory / "lifetimes.tsv").read_text().splitlines()]
    if any(stock != index for index, (stock, _, _) in enumerate(lives)):
        raise ValueError("Unexpected ticker numbering")
    records = sum(end - birth for _, birth, end in lives)
    tail = sum(max(0, end - max(birth, configuration["boundary"]))
               for _, birth, end in lives)
    if records != layout["records"] or tail != layout["tail_records"]:
        raise ValueError("Lifecycle record counts disagree with the layout model")
    if any(completed[key] != value for key, value in layout.items()):
        raise ValueError("Completed layout differs from the recorded fixture model")
    validation = only_event(events, "driver", stage="tail_validation")
    if validation["verified_rows"] != tail:
        raise ValueError("Incomplete tail validation")
    loads = [row for row in events if row["event"] == "driver" and row["stage"] == "load"]
    if sum(row["rows"] for row in loads) != tail:
        raise ValueError("Incomplete tail import")
    registrations = [row for row in events if row["event"] == "driver"
                     and row["stage"] == "register"]
    if sum(row["end"] - row["first"] for row in registrations) != len(lives) - 10000:
        raise ValueError("Incorrect registration count")
    startup = only_event(events, "driver", stage="reopen_before_reads")
    if startup["tickers"] != len(lives):
        raise ValueError("Reopened dictionary has an incorrect count")
    lifecycle = [row for row in events if row["event"] == "lifecycle_check"]
    expected_checks = set()
    if configuration["retired_per_event"]:
        expected_checks.update(("after_retirement", "retired_history"))
    if configuration["replacement"]:
        expected_checks.update(("before_listing", "after_listing"))
    if {row["label"] for row in lifecycle} != expected_checks or len(lifecycle) != len(
            expected_checks):
        raise ValueError("Missing or duplicate lifecycle boundary checks")
    for row in lifecycle:
        expected_rows = 0 if row["label"] in ("after_retirement", "before_listing") else 240
        if row["rows"] != expected_rows:
            raise ValueError("Incorrect lifecycle boundary result")

    warm_records = read_lines(directory / "warm.jsonl")
    cold_records = read_lines(directory / "cold.jsonl")
    if len(warm_records) != 672 or len(cold_records) != 12:
        raise ValueError("Incomplete query campaign")
    warm = {}
    cold = {}
    for label in LABELS:
        measured = [row for row in warm_records if row["label"] == label and row["measured"]]
        first_queries = [row for row in cold_records if row["label"] == label]
        if len(measured) != 120 or len(first_queries) != 3:
            raise ValueError("Incorrect sample count")
        expected = {"cross_recent": layout["final_active"], "cross_history": 10000,
                    "series240": 240, "series2048": 2048}[label]
        for row in measured + first_queries:
            if row["rows"] != expected:
                raise ValueError(f"Unexpected result size: {directory} {label}")
        warm[label] = {key: distribution([row[key] for row in measured])
                       for key in ("wall_ns", "read_bytes", "read_calls", "cache_hits", "cache_misses")}
        warm[label]["rows"] = expected
        cold[label] = {key: distribution([row[key] for row in first_queries])
                       for key in ("wall_ns", "read_bytes", "read_calls", "startup_ns")}
        cold[label]["rows"] = expected
        cold[label]["max_remaining_pages"] = max(row["eviction"]["resident_after"]
                                                   for row in first_queries)
        cold[label]["read_bytes_per_payload_byte"] = (
            cold[label]["read_bytes"]["median"] / (expected * 32))

    files = json.loads((directory / "ohlc-files.json").read_text())
    categories = {"data": 0, "metadata": 0, "index": 0, "catalog": 0, "wal": 0, "other": 0}
    physical_slots = 0
    for entry in files:
        path = Path(entry["path"])
        if path.name.startswith("data-") and path.suffix == ".dat":
            key = "data"
            if (entry["logical"] - 4096) % 4096:
                raise ValueError("Unaligned data volume")
            physical_slots += (entry["logical"] - 4096) // 4096
        elif path.suffix == ".meta":
            key = "metadata"
        elif path.name.startswith("index-"):
            key = "index"
        elif path.name.startswith("catalog-"):
            key = "catalog"
        elif path.name.startswith("wal-"):
            key = "wal"
        else:
            key = "other"
        categories[key] += entry["logical"]
    storage = only_event(events, "storage")
    if sum(categories.values()) != storage["logical_bytes"]:
        raise ValueError("Storage inventory mismatch")
    if sum(row["allocated"] for row in files) != storage["allocated_bytes"]:
        raise ValueError("Allocated storage mismatch")
    if physical_slots < layout["current_tiles"]:
        raise ValueError("Data volumes are smaller than the current tile model")
    resources = read_lines(directory / "ohlc-resources.jsonl")
    oom = []
    for row in resources:
        counters = dict(line.split() for line in row.get("memory.events", "").splitlines())
        oom.append(int(counters.get("oom_kill", 0)))
    memory = {"engine_after_reopen_bytes": startup["memory_bytes"],
              "cold_start_rss_bytes": distribution([row["process_before"]["VmRSS_bytes"]
                                                    for row in cold_records]),
              "observed_rss_peak_bytes": max(row.get("rss_sum_bytes", row.get("VmRSS_bytes", 0))
                                              for row in resources),
              "observed_cgroup_peak_bytes": max(int(row["memory.current"]) for row in resources),
              "oom_kills": max(oom), "observations": len(resources)}
    if memory["oom_kills"]:
        raise ValueError("Observed OOM during a supposedly completed case")
    return {"configuration": configuration, "layout": layout, "warm": warm, "cold": cold,
            "memory": memory, "storage": storage, "file_bytes": categories,
            "appended_data_slots": physical_slots,
            "retained_extra_data_bytes": (physical_slots - layout["current_tiles"]) * 4096,
            "lifecycle_checks": lifecycle, "tail_validation": validation,
            "registration_ns": sum(row["wall_ns"] for row in registrations),
            "tail_write_ns": sum(row["wall_ns"] for row in loads),
            "tail_request_ns": sum(row["request_ns"] for row in loads)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path, help="Directory containing the main campaign")
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    root = arguments.root / "main"
    campaign = json.loads((root / "campaign.json").read_text())
    completed = json.loads((root / "completed.json").read_text())
    if campaign["smoke"] or len(campaign["cases"]) != 15 or completed["cases"] != 15:
        raise ValueError("This summary requires the full 15-case campaign")
    seed_events = read_lines(root / "seed/results/events.jsonl")
    seed_validation = only_event(seed_events, "driver", stage="seed_validation")
    if seed_validation["verified_rows"] != 40_960_000:
        raise ValueError("Incomplete seed validation")
    cases = {name: summarize_case(root / name / "results") for name in campaign["cases"]}
    for filename in ("warm.jsonl", "cold.jsonl"):
        expected_targets = None
        for name in campaign["cases"]:
            records = read_lines(root / name / "results" / filename)
            targets = [(row["label"], row["kind"], row["stock"], row["first"], row["length"])
                       for row in records]
            if expected_targets is not None and targets != expected_targets:
                raise ValueError("Query targets differ between lifecycle cases")
            expected_targets = targets
    summary = {"campaign": campaign, "completed": completed,
               "seed_validation": seed_validation, "cases": cases}
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    print(f"Verified all 15 lifecycle cases: {arguments.output}")


if __name__ == "__main__":
    main()
