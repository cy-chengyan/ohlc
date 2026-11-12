#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Execute isolated OHLC acceptance workloads as the ohlc account on Linux."""

import argparse
from dataclasses import dataclass
import json
import os
from pathlib import Path
import random
import struct
import sys
import time

import numpy as np

from benchmark_acceptance_data import FileCache, Workload, space_snapshot
from benchmark_competitors import Ohlc, ROW
from benchmark_metrics import atomic_json, cgroup_swap_snapshot, proc_snapshot, summarize
from benchmark_quotation import RESULT

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "clients/python"))
from ohlc import Connection
from ohlc.embedded import Database, DatabaseOptions


class Events:
    def __init__(self, path):
        self.stream = path.open("x", buffering=1)

    def __call__(self, event, **values):
        self.stream.write(json.dumps({"event": event, "time": time.time(),
                                      "monotonic_ns": time.monotonic_ns(), **values}) + "\n")

    def close(self):
        self.stream.close()


@dataclass
class ReadResult:
    rows: list
    raw: bytes
    sequence: int
    wall_ns: int

    def verify(self, expected):
        if self.raw != expected.tobytes():
            raise RuntimeError("Query keys, order, row count, fields, or validity bits differ")


class Session:
    """Own one native connection; decode every returned field before ending its timer."""

    def __init__(self, args, create=False):
        self.network = args.mode == "network"
        self.adapter = None
        started = time.monotonic_ns()
        if self.network:
            self.adapter = Ohlc(args.binary, args.stocks, args.port)
            self.owner = self.adapter.client
            if create:
                self.adapter.create()
            else:
                self.adapter.open_table()
            self.table = self.adapter.table
        else:
            self.owner = Database(args.data, options=DatabaseOptions(
                memory_limit=args.memory_mib << 20, cache_bytes=args.cache_mib << 20,
                max_query_ms=300000, read_workers=args.read_workers),
                library=args.binary.parent / "libohlc.so")
            self.table = self.owner.table("comparison")
        self.open_ns = time.monotonic_ns() - started

    def read(self, kind, stock, first_key, last_key):
        if self.network:
            function, arguments, query = self.adapter.prepare_query(
                kind, stock, first_key, last_key)
        else:
            arguments = (self.table.id, None if kind == "X" else stock, first_key,
                         0 if kind == "X" else last_key)
        started = time.monotonic_ns()
        if self.network:
            with self.owner._lock:
                self.owner._check(function(*arguments))
        else:
            query = self.owner._query(*arguments)
        rows = []
        parts = []
        sequences = set()
        with query:
            for chunk in query:
                rows.extend(ROW.iter_unpack(chunk.data))
                parts.append(chunk.data)
                sequences.add(chunk.snapshot_sequence)
        raw = b"".join(parts)
        elapsed = time.monotonic_ns() - started
        if len(sequences) != 1:
            raise RuntimeError("Query chunks do not share a single snapshot sequence")
        return ReadResult(rows, raw, sequences.pop(), elapsed)

    def close(self):
        self.owner.close()


def record_state(args, log, session, label, times=None):
    swap = cgroup_swap_snapshot(os.getpid())
    if swap.get("current_bytes", 0) or swap.get("peak_bytes", 0):
        raise RuntimeError("Acceptance worker swapped")
    strict = getattr(args, "action", None) == "read" or label in (
        "after_import", "after_mixed_checkpoint")
    state = space_snapshot(args.data, args.stocks if times is not None else None,
                           times, strict=strict)
    if state["filesystem_free_bytes"] < 64 << 30:
        raise RuntimeError("Filesystem reserve fell below 64 GiB")
    log("state", label=label, engine=session.owner.stats(),
        process=proc_snapshot(os.getpid()), space=state, swap=swap)


def import_rows(args, log):
    workload = Workload(args.seed, args.stocks, args.times)
    log("workload", **workload.describe())
    session = Session(args, create=True)
    start = time.monotonic_ns()
    last_state = time.monotonic()
    write_ns = 0
    try:
        for first in range(0, args.times, 8):
            count = min(8, args.times - first)
            rows = workload.batch(first, count)
            started = time.monotonic_ns()
            sequence = session.table.write_encoded(rows)
            elapsed = time.monotonic_ns() - started
            write_ns += elapsed
            log("import_batch", first=first, times=count, rows=len(rows),
                sequence=sequence, write_ns=elapsed)
            if time.monotonic() - last_state >= 30:
                record_state(args, log, session, "import", first + count)
                last_state = time.monotonic()
        settling = time.monotonic_ns()
        session.owner.checkpoint()
        settle_ns = time.monotonic_ns() - settling
        log("import_complete", rows=args.stocks * args.times,
            wall_ns=time.monotonic_ns() - start, write_ns=write_ns, settle_ns=settle_ns)
        record_state(args, log, session, "after_import", args.times)
        for first in (0, args.times // 2, args.times - 1):
            key = int(workload.keys[first])
            session.read("X", 0, key, key).verify(workload.expected("X", 0, first, 1))
        log("import_probes_passed", cross_sections=3)
    finally:
        session.close()


def read_cases(args, log):
    workload = Workload(args.seed, args.stocks, args.times)
    log("workload", **workload.describe())
    session = Session(args)
    cache = FileCache(args.data)
    try:
        log("open", wall_ns=session.open_ns, engine=session.owner.stats())
        started = time.monotonic_ns()
        residency = cache.warm() if args.cache_state == "hot" else cache.evict()
        log("cache_preparation", cache_state=args.cache_state, residency=residency,
            wall_ns=time.monotonic_ns() - started)
        if args.cache_state == "natural":
            memory = int(next(line.split()[1] for line in Path("/proc/meminfo").read_text().splitlines()
                              if line.startswith("MemTotal:"))) * 1024
            if residency["data_volume_bytes"] <= memory and not args.smoke:
                raise RuntimeError("Natural-cold dataset does not exceed host physical memory")
            log("over_memory", host_memory_bytes=memory,
                data_volume_bytes=residency["data_volume_bytes"],
                engine_memory_limit=args.memory_mib << 20)
        for label, requested in (("R1", 1), ("R60", 60), ("R240", 240),
                                 ("R4800", 4800), ("RFULL", args.times), ("X", 0)):
            length = min(requested, args.times)
            kind = "X" if length == 0 else "S"
            count = max(1, length)
            rng = random.Random(20260926 + count)
            values = []
            begin = time.monotonic()
            for sample in range(5 if args.smoke else 1000):
                first = rng.randrange(args.times - count + 1)
                stock = rng.randrange(args.stocks)
                expected = workload.expected(kind, stock, first, count)
                if args.cache_state == "cold":
                    observed = cache.evict()
                else:
                    observed = None
                before = session.owner.stats()
                process_before = proc_snapshot(os.getpid())
                actual = session.read(kind, stock, int(workload.keys[first]),
                                      int(workload.keys[first + count - 1]))
                after = session.owner.stats()
                verification = time.monotonic_ns()
                actual.verify(expected)
                verify_ns = time.monotonic_ns() - verification
                blocks = (args.stocks + 15) // 16 if kind == "X" else (
                    (first % 8 + count + 7) // 8)
                read_bytes = after["disk_read_bytes"] - before["disk_read_bytes"]
                if args.cache_state == "cold" and read_bytes != blocks * 8192:
                    raise RuntimeError(f"Cold read amplification differs: {read_bytes} != {blocks * 8192}")
                values.append(actual.wall_ns)
                log("query", label=label, sample=sample, first=first, stock=stock,
                    rows=len(actual.rows), wall_ns=actual.wall_ns, verify_ns=verify_ns,
                    sequence=actual.sequence, residency_before=observed,
                    expected_blocks=blocks, engine_read_bytes=read_bytes,
                    engine_read_calls=after["disk_read_calls"] - before["disk_read_calls"],
                    process_read_bytes=proc_snapshot(os.getpid()).get("read_bytes", 0) -
                                       process_before.get("read_bytes", 0))
                if sample >= 199 and time.monotonic() - begin > 120:
                    break
            log("latency_summary", label=label, cache_state=args.cache_state,
                wall_ns=summarize(values), residency_after=cache.residency())
            record_state(args, log, session, label, args.times)
    finally:
        cache.close()
        session.close()


def verify_prefix(args, log):
    workload = Workload(args.seed, args.stocks, args.times)
    session = Session(args)
    checked = 0
    try:
        for first in range(args.verify_first, args.times):
            key = int(workload.keys[first])
            actual = session.read("X", 0, key, key)
            actual.verify(workload.expected("X", 0, first, 1))
            checked += len(actual.rows)
            if (first - args.verify_first) % 1000 == 0:
                log("verify_progress", first=first, checked_rows=checked)
        log("verification_complete", checked_rows=checked,
            first=args.verify_first, times=args.times, engine=session.owner.stats())
    finally:
        session.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("import", "read", "verify"))
    parser.add_argument("--seed", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--stocks", type=int, default=10000)
    parser.add_argument("--times", type=int, default=15661)
    parser.add_argument("--port", type=int, default=18766)
    parser.add_argument("--mode", choices=("network", "embedded"), default="network")
    parser.add_argument("--memory-mib", type=int, default=24576)
    parser.add_argument("--cache-mib", type=int, default=64)
    parser.add_argument("--read-workers", type=int, choices=(0, 8), default=0)
    parser.add_argument("--cache-state", choices=("hot", "cold", "natural"), default="hot")
    parser.add_argument("--verify-first", type=int, default=0)
    parser.add_argument("--smoke", action="store_true")
    args = parser.parse_args()
    if args.action == "read" and args.cache_state == "cold" and args.cache_mib:
        parser.error("Controlled cold reads require cache-mib=0")
    log = Events(args.output)
    try:
        {"import": import_rows, "read": read_cases, "verify": verify_prefix}[args.action](args, log)
        log("complete", status="passed")
    except BaseException as error:
        log("failed", error=repr(error))
        raise
    finally:
        log.close()


if __name__ == "__main__":
    main()
