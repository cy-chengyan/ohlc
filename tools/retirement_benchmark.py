# SPDX-License-Identifier: Apache-2.0
"""Measure ticker retirement, sparse tiles and replacement cohorts on Linux."""

import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import random
import select
import shutil
import subprocess
import sys

from baseline_compare import Experiment, ResourceSampler, data_files, evict_files, process_sample


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(8 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def fixture(stocks, times, boundary, percent=0, placement="grouped", replace=False, churn=False):
    lives = [[0, times] for _ in range(stocks)]
    retired = (stocks // 16 * percent // 100) * 16
    events = []
    boundaries = [boundary]
    if churn:
        step = 512 if times > 512 else 64
        boundaries = list(range(boundary, times, step))
    anchors = set(range(stocks - 16, stocks))
    if retired:
        for point in boundaries:
            eligible = [index for index, (_, end) in enumerate(lives)
                        if end == times and index not in anchors]
            if placement == "grouped":
                selected = eligible[:retired]
                if any(selected[index + 15] - selected[index] != 15
                       or selected[index] % 16 for index in range(0, retired, 16)):
                    raise ValueError("The grouped retirement must select complete groups")
            else:
                selected = [eligible[index * len(eligible) // retired]
                            for index in range(retired)]
            if len(set(selected)) != retired:
                raise ValueError("The retirement cohort contains duplicates")
            for index in selected:
                lives[index][1] = point
            first_new = len(lives)
            if replace:
                lives.extend([[point, times] for _ in selected])
            events.append({"time": point, "retired": retired,
                           "first_new": first_new, "end_new": len(lives)})
    return {"initial_stocks": stocks, "times": times, "boundary": boundary,
            "requested_percent": percent, "retired_per_event": retired,
            "actual_percent": retired * 100 / stocks, "placement": placement,
            "replacement": replace, "churn": churn, "events": events, "lives": lives,
            "permanent_query_stock": stocks - 1}


def occupied_tiles(lives, first, end):
    groups = {}
    for stock, (birth, death) in enumerate(lives):
        start = max(first, birth)
        stop = min(end, death)
        if start < stop:
            groups.setdefault(stock // 16, []).append((start // 8, (stop + 7) // 8))
    tiles = 0
    for intervals in groups.values():
        high = -1
        for low, stop in sorted(intervals):
            if stop > high:
                tiles += stop - max(low, high)
                high = stop
    return tiles


def model(configuration):
    lives = configuration["lives"]
    times = configuration["times"]
    boundary = configuration["boundary"]
    records = sum(end - birth for birth, end in lives)
    tail_records = sum(max(0, end - max(birth, boundary)) for birth, end in lives)
    current_tiles = occupied_tiles(lives, 0, times)
    tail_tiles = occupied_tiles(lives, boundary, times)
    final_groups = {stock // 16 for stock, (birth, end) in enumerate(lives)
                    if birth <= times - 1 < end}
    return {"records": records, "tail_records": tail_records, "tickers": len(lives),
            "final_active": sum(end == times for _, end in lives),
            "final_active_groups": len(final_groups), "current_tiles": current_tiles,
            "tail_tiles": tail_tiles, "current_data_bytes": current_tiles * 4096,
            "tail_data_bytes": tail_tiles * 4096,
            "current_payload_fill": records * 32 / (current_tiles * 4096),
            "tail_payload_fill": tail_records * 32 / (tail_tiles * 4096)}


def save_fixture(directory, configuration):
    directory.mkdir(parents=True, exist_ok=True)
    lives = configuration["lives"]
    (directory / "lifetimes.tsv").write_text("".join(
        f"{stock}\t{birth}\t{end}\n" for stock, (birth, end) in enumerate(lives)))
    write_json(directory / "fixture.json", {key: value for key, value in configuration.items()
                                            if key != "lives"})
    write_json(directory / "model.json", model(configuration))


class Driver:
    def __init__(self, run, create=False):
        self.run = run
        self.errors = (run.results / "driver.stderr").open("a")
        command = ["taskset", "-c", "16-23", run.root / "retirement-driver", "18765",
                   run.results / "lifetimes.tsv", str(run.times), "create" if create else "open"]
        self.process = subprocess.Popen([str(item) for item in command], text=True, bufsize=1,
                                        stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=self.errors)
        if self.response().get("operation") != "ready":
            raise RuntimeError("The driver did not become ready")

    def response(self):
        if not select.select([self.process.stdout], [], [], 600)[0]:
            self.process.kill()
            raise RuntimeError("Benchmark driver timed out")
        line = self.process.stdout.readline()
        if not line:
            raise RuntimeError("Benchmark driver stopped; inspect driver.stderr")
        return json.loads(line)

    def command(self, command):
        self.process.stdin.write(command + "\n")
        self.process.stdin.flush()
        result = self.response()
        expected = {"R": "register", "W": "write", "Q": "query", "V": "validate",
                    "T": "stats", "C": "checkpoint"}[command[0]]
        if result["operation"] != expected:
            raise RuntimeError("Unexpected driver response")
        return result

    def query(self, kind, stock, first, length):
        return self.command(f"Q {kind} {stock} {first} {length}")

    def close(self):
        if self.process.poll() is None:
            self.process.stdin.write("STOP\n")
            self.process.stdin.flush()
        self.process.wait(timeout=10)
        self.errors.close()
        if self.process.returncode != 0:
            raise RuntimeError("The benchmark driver failed")


class Run(Experiment):
    def __init__(self, root, baseline, name, configuration):
        super().__init__(root, name, configuration["initial_stocks"], configuration["times"])
        self.baseline = baseline
        self.configuration = configuration
        save_fixture(self.results, configuration)

    def command(self, backend):
        if backend != "ohlc":
            raise ValueError("This experiment only runs ohlc")
        return [self.baseline / "build/ohlcd", "--data", self.work / "ohlc",
                "--host", "127.0.0.1", "--port", "18765", "--memory-mib", "1024",
                "--cache-mib", "64", "--connections", "64", "--timeout-ms", "120000",
                "--query-ms", "120000"]

    def capture(self, driver, command, stage):
        value = driver.command(command)
        self.record("driver", stage=stage, **value)
        return value


def fingerprint_directory(directory):
    return [{"path": str(path.relative_to(directory)), "bytes": path.stat().st_size,
             "sha256": sha256(path)} for path in sorted(data_files(directory))]


def create_seed(root, baseline, configuration):
    run = Run(root, baseline, "seed", configuration)
    boundary = configuration["boundary"]
    try:
        run.start("ohlc")
        driver = Driver(run, create=True)
        try:
            run.capture(driver, f"R 0 {configuration['initial_stocks']}", "seed_register")
            run.capture(driver, f"W 0 {boundary}", "seed_load")
            run.capture(driver, "C", "seed_checkpoint")
            validated = run.capture(driver, f"V 0 {boundary}", "seed_validation")
            if validated["verified_rows"] != configuration["initial_stocks"] * boundary:
                raise RuntimeError("Seed row count mismatch")
            run.capture(driver, "T", "seed_stats")
        finally:
            driver.close()
        run.stop("ohlc")
        run.inventory("ohlc")
        write_json(run.results / "files-sha256.json", fingerprint_directory(run.work / "ohlc"))
        run.record("seed_completed")
    finally:
        run.close()


def query_target(configuration, label, randomizer):
    times = configuration["times"]
    boundary = configuration["boundary"]
    stock = configuration["permanent_query_stock"]
    width = min(128, times - boundary)
    if label == "cross_recent":
        return "X", 0, randomizer.randrange(times - width, times), 1
    if label == "cross_history":
        return "X", 0, randomizer.randrange(max(0, boundary - 128), boundary), 1
    length = min(240 if label == "series240" else 2048, times - boundary)
    first = randomizer.randrange(boundary, times - length + 1)
    return "S", stock, first, length


def lifecycle_checks(run, driver):
    configuration = run.configuration
    times = configuration["times"]
    lives = configuration["lives"]
    for stock, (birth, end) in enumerate(lives):
        if end < times:
            result = driver.query("S", stock, end, times - end)
            run.record("lifecycle_check", label="after_retirement", **result)
            result = driver.query("S", stock, max(birth, end - 240), min(240, end - birth))
            run.record("lifecycle_check", label="retired_history", **result)
            break
    for stock, (birth, end) in enumerate(lives):
        if birth:
            result = driver.query("S", stock, 0, birth)
            run.record("lifecycle_check", label="before_listing", **result)
            result = driver.query("S", stock, birth, min(240, end - birth))
            run.record("lifecycle_check", label="after_listing", **result)
            break


def warm_queries(run, driver, smoke):
    randomizer = random.Random(20260927)
    labels = ("cross_recent", "cross_history", "series240", "series2048")
    with (run.results / "warm.jsonl").open("w") as output:
        for round_number in range(1 if smoke else 3):
            for label in labels:
                for index in range(3 if smoke else 56):
                    result = driver.query(*query_target(run.configuration, label, randomizer))
                    result.update(label=label, round=round_number,
                                  measured=index >= (1 if smoke else 16))
                    output.write(json.dumps(result) + "\n")
    run.record("warm_completed")


def cold_queries(run, smoke):
    randomizer = random.Random(3102026)
    labels = ("cross_recent", "cross_history", "series240", "series2048")
    with (run.results / "cold.jsonl").open("w") as output:
        for label in labels:
            for repetition in range(1 if smoke else 3):
                run.stop("ohlc")
                eviction = evict_files(run.work / "ohlc")
                startup = run.start("ohlc")
                driver = Driver(run)
                try:
                    before = driver.command("T")
                    resident = process_sample(startup["pid"])
                    result = driver.query(*query_target(run.configuration, label, randomizer))
                    result.update(label=label, repetition=repetition, eviction=eviction,
                                  startup_ns=startup["startup_ns"], engine_before=before,
                                  process_before=resident)
                    output.write(json.dumps(result) + "\n")
                    output.flush()
                finally:
                    driver.close()
    run.record("cold_completed")


def execute_case(root, baseline, name, configuration, smoke):
    run = Run(root, baseline, name, configuration)
    sampler = None
    try:
        seed = root / "seed/ohlc"
        shutil.copytree(seed, run.work / "ohlc")
        expected = json.loads((root / "seed/results/files-sha256.json").read_text())
        if fingerprint_directory(run.work / "ohlc") != expected:
            raise RuntimeError("Copied seed fingerprint mismatch")
        for path in data_files(run.work / "ohlc"):
            with path.open("rb") as source:
                os.fsync(source.fileno())
        run.record("seed_copy_verified", files=len(expected))
        run.start("ohlc")
        sampler = ResourceSampler(run, "ohlc")
        sampler.start()
        driver = Driver(run)
        try:
            first = configuration["boundary"]
            events = {event["time"]: event for event in configuration["events"]}
            boundaries = sorted({first, configuration["times"], *events})
            for index, point in enumerate(boundaries[:-1]):
                event = events.get(point)
                if event and event["end_new"] > event["first_new"]:
                    run.capture(driver, f"R {event['first_new']} {event['end_new']}", "register")
                run.capture(driver, f"W {point} {boundaries[index + 1]}", "load")
            run.capture(driver, "C", "checkpoint")
        finally:
            driver.close()
        run.stop("ohlc")
        sampler.stop()
        sampler = None
        run.start("ohlc")
        sampler = ResourceSampler(run, "ohlc")
        sampler.start()
        driver = Driver(run)
        try:
            run.capture(driver, "T", "reopen_before_reads")
            result = run.capture(driver, f"V {first} {configuration['times']}", "tail_validation")
            if result["verified_rows"] != model(configuration)["tail_records"]:
                raise RuntimeError("Lifecycle tail count mismatch")
            lifecycle_checks(run, driver)
            warm_queries(run, driver, smoke)
        finally:
            driver.close()
        sampler.stop()
        sampler = None
        cold_queries(run, smoke)
        run.stop("ohlc")
        run.inventory("ohlc")
        run.record("case_completed", **model(configuration))
        shutil.rmtree(run.work / "ohlc")
        run.record("temporary_data_removed")
    except Exception as error:
        run.record("failed", error=str(error))
        raise
    finally:
        if sampler is not None:
            sampler.stop()
        run.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--smoke", action="store_true")
    arguments = parser.parse_args()
    root = arguments.root.resolve()
    baseline = arguments.baseline.resolve()
    if sys.platform != "linux" or Path("/ssd02") not in root.parents:
        parser.error("Use a dedicated Linux /ssd02 directory")
    if (root / "campaign.json").exists():
        parser.error("The campaign directory has already been used")
    stocks = 64 if arguments.smoke else 10000
    times = 256 if arguments.smoke else 8580
    boundary = 128 if arguments.smoke else 4096
    percentages = (25, 50, 75) if arguments.smoke else (10, 50, 90)
    configurations = {"dense": fixture(stocks, times, boundary)}
    for replace in (False, True):
        for percent in percentages:
            for placement in ("grouped", "scattered"):
                name = f"{'replace' if replace else 'retire'}-{placement}-{percent}"
                configurations[name] = fixture(stocks, times, boundary, percent, placement, replace)
    for placement in ("grouped", "scattered"):
        name = f"churn-{placement}-50"
        configurations[name] = fixture(stocks, times, boundary, 50, placement, True, True)
    root.mkdir(parents=True, exist_ok=True)
    write_json(root / "campaign.json", {"started_utc": datetime.datetime.now(
        datetime.timezone.utc).isoformat(), "smoke": arguments.smoke, "cases": list(configurations),
        "baseline": str(baseline), "server_cpus": "0-15", "client_cpus": "16-23",
        "ohlcd_sha256": sha256(baseline / "build/ohlcd"),
        "driver_sha256": sha256(root / "retirement-driver")})
    create_seed(root, baseline, configurations["dense"])
    for name, configuration in configurations.items():
        execute_case(root, baseline, name, configuration, arguments.smoke)
    shutil.rmtree(root / "seed/ohlc")
    write_json(root / "completed.json", {"completed_utc": datetime.datetime.now(
        datetime.timezone.utc).isoformat(), "cases": len(configurations), "test_data_removed": True})
    print("Lifecycle campaign completed and temporary data removed", flush=True)


if __name__ == "__main__":
    main()
