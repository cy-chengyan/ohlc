#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Sustained reads, acknowledged appends, deterministic revisions, and complete final audit."""

import argparse
import gzip
import hashlib
import json
import multiprocessing as mp
import os
from pathlib import Path
import queue
import random
import time
import traceback

import numpy as np

from benchmark_acceptance import Events, Session, record_state
from benchmark_acceptance_data import FileCache, Workload
from benchmark_metrics import atomic_json, cgroup_swap_snapshot, proc_snapshot, summarize
from benchmark_quotation import RESULT


def reader(args, index, state, start, stop, messages):
    """Verify snapshots against the acknowledged lower bound and published revision intents."""
    os.sched_setaffinity(0, {24 + index})
    workload = Workload(args.seed, args.stocks, args.times + int(args.duration) + 256)
    session = None
    log = Events(args.output.parent / f"reader-{index}.jsonl")
    samples = gzip.open(args.output.parent / f"reader-{index}-latency-ns.u64.gz", "xb", compresslevel=1)
    rng = random.Random(20261111 + index)
    kind = "S" if index < args.readers // 2 else "X"
    latency = []
    requests = 0
    verified = 0
    verify_ns = 0
    total_samples = 0
    phase = 0
    window_start = time.monotonic_ns()

    def flush_window(final=False):
        nonlocal latency, verified, verify_ns, total_samples, window_start
        now = time.monotonic_ns()
        if latency:
            samples.write(np.asarray(latency, dtype="<u8").tobytes())
            samples.flush()
            log("read_window", reader=index, kind=kind, phase=phase,
                start_ns=window_start, duration_ns=now - window_start,
                requests=len(latency), qps=len(latency) * 1e9 / (now - window_start),
                latency_ns=summarize(latency), verified_queries=verified,
                verify_ns=verify_ns, sample_offset=total_samples, final=final,
                process=proc_snapshot(os.getpid()), swap=cgroup_swap_snapshot(os.getpid()))
            total_samples += len(latency)
        latency = []
        verified = 0
        verify_ns = 0
        window_start = now

    try:
        session = Session(args)
        messages.put({"event": "reader_ready", "reader": index})
        if not start.wait(300):
            raise TimeoutError("Mixed reader start gate")
        window_start = time.monotonic_ns()
        while not stop.is_set():
            next_phase = state["phase"].value
            if next_phase != phase:
                flush_window()
                phase = next_phase
            with state["lock"]:
                last = state["latest"].value
                maximum_revision = state["revision"].value
                sequence_lower_bound = state["sequence"].value
                if kind == "X":
                    if requests % 4 == 0 and maximum_revision:
                        first = state["targets"][rng.randrange(1, maximum_revision + 1)]
                    elif requests % 4 == 1:
                        first = last
                    else:
                        first = rng.randrange(last + 1)
                    count = 1
                    required = state["revisions"][first] if first < args.times else 0
                else:
                    count = min(240, last + 1)
                    first = rng.randrange(last - count + 2)
                    required = np.zeros(count, dtype=np.int64)
                    upper = min(first + count, args.times)
                    if first < upper:
                        required[:upper - first] = state["revisions"][first:upper]
            stock = rng.randrange(args.stocks)
            actual = session.read(kind, stock, int(workload.keys[first]),
                                  int(workload.keys[first + count - 1]))
            expected_count = args.stocks if kind == "X" else count
            expected_first = 0 if kind == "X" else int(workload.keys[first])
            expected_last = args.stocks - 1 if kind == "X" else int(workload.keys[first + count - 1])
            if (len(actual.rows) != expected_count or actual.rows[0][0] != expected_first or
                    actual.rows[-1][0] != expected_last or actual.sequence < sequence_lower_bound):
                raise RuntimeError("Mixed query count, key boundary, or acknowledged snapshot regressed")
            started = time.monotonic_ns()
            if requests % 16 == 0:
                rows = np.frombuffer(actual.raw, dtype=RESULT)
                revisions = rows["value"]["reserved"].astype(np.int64)
                if np.any(revisions < required):
                    raise RuntimeError("Mixed query returned a revision older than its acknowledged lower bound")
                if kind == "X" and np.any(revisions != revisions[0]):
                    raise RuntimeError("Atomic cross-section replacement was partially visible")
                positions = np.full(expected_count, first) if kind == "X" else np.arange(first, first + count)
                with state["lock"]:
                    maximum_revision = state["revision"].value
                    for revision in np.unique(revisions):
                        revision = int(revision)
                        if revision < 0 or revision > maximum_revision:
                            raise RuntimeError("Read an unpublished revision")
                        if revision and np.any(positions[revisions == revision] != state["targets"][revision]):
                            raise RuntimeError("Revision belongs to a different timestamp")
                actual.verify(workload.expected(kind, stock, first, count, revisions))
                verified += 1
            verify_ns += time.monotonic_ns() - started
            latency.append(actual.wall_ns)
            requests += 1
            if time.monotonic_ns() - window_start >= 60_000_000_000:
                flush_window()
        flush_window(final=True)
        log("complete", status="passed", requests=requests, samples=total_samples)
        messages.put({"event": "reader_complete", "reader": index, "requests": requests})
    except BaseException as error:
        log("failed", error=repr(error), traceback=traceback.format_exc())
        messages.put({"event": "reader_failed", "reader": index, "error": repr(error)})
        stop.set()
        raise
    finally:
        if session is not None:
            session.close()
        samples.close()
        log.close()


def ensure_readers(processes, stop):
    if stop.is_set() or any(not process.is_alive() for process in processes):
        raise RuntimeError("A mixed reader stopped before the writer completed")


def mixed(args, log):
    os.sched_setaffinity(0, {32})
    workload = Workload(args.seed, args.stocks, args.times + int(args.duration) + 256)
    log("workload", base_times=args.times, duration_seconds=args.duration,
        readers=args.readers, baseline_seconds=args.baseline,
        correction_interval=args.correction_interval, **workload.describe())
    context = mp.get_context("spawn")
    state = {"lock": context.Lock(), "latest": context.Value("q", args.times - 1, lock=False),
             "phase": context.Value("i", 0, lock=False),
             "sequence": context.Value("q", 0, lock=False),
             "revision": context.Value("q", 0, lock=False),
             "revisions": context.Array("q", args.times, lock=False),
             "targets": context.Array("q", int(args.duration) // args.correction_interval + 256,
                                      lock=False)}
    start = context.Event()
    stop = context.Event()
    messages = context.Queue()
    processes = [context.Process(target=reader, args=(args, index, state, start, stop, messages))
                 for index in range(args.readers)]
    session = Session(args)
    ledger = (args.output.parent / "writer-ledger.jsonl").open("x", buffering=1)
    confirmed = 0
    appends = 0
    revision = 0
    writes = []
    rng = random.Random(20261111)

    def commit(first, revision, operation):
        nonlocal confirmed
        rows = workload.batch(first, revision=revision)
        intent = {"event": "intent", "operation": operation, "first": first,
                  "revision": revision, "rows": len(rows),
                  "sha256": hashlib.sha256(rows.tobytes()).hexdigest(), "time": time.time()}
        ledger.write(json.dumps(intent) + "\n")
        ledger.flush()
        os.fsync(ledger.fileno())
        before = session.owner.stats()
        started = time.monotonic_ns()
        sequence = session.table.write_encoded(rows)
        elapsed = time.monotonic_ns() - started
        acknowledged = {"event": "ack", "operation": operation, "first": first,
                        "revision": revision, "sequence": sequence, "wall_ns": elapsed,
                        "time": time.time()}
        ledger.write(json.dumps(acknowledged) + "\n")
        ledger.flush()
        os.fsync(ledger.fileno())
        with state["lock"]:
            state["sequence"].value = sequence
            if operation == "append":
                state["latest"].value = first
            else:
                state["revisions"][first] = revision
        after = session.owner.stats()
        log("write", **{key: value for key, value in acknowledged.items() if key != "event"},
            checkpoint_before=before["checkpoint_seq"], checkpoint_after=after["checkpoint_seq"],
            wal_bytes=after["wal_bytes"] - before["wal_bytes"],
            data_bytes=after["data_bytes"] - before["data_bytes"])
        confirmed += 1
        writes.append(elapsed)

    try:
        record_state(args, log, session, "before_baseline", args.times)
        for process in processes:
            process.start()
        for _ in processes:
            message = messages.get(timeout=300)
            if message["event"] != "reader_ready":
                raise RuntimeError(str(message))
        start.set()
        baseline_end = time.monotonic() + args.baseline
        while time.monotonic() < baseline_end:
            ensure_readers(processes, stop)
            time.sleep(.2)
        record_state(args, log, session, "after_baseline", args.times)
        cache = FileCache(args.data)
        try:
            log("baseline_cache_residency", **cache.residency())
        finally:
            cache.close()
        state["phase"].value = 1
        started = time.monotonic()
        deadline = started + args.duration
        next_batch = started
        next_state = started + 60
        while time.monotonic() < deadline:
            ensure_readers(processes, stop)
            commit(args.times + appends, 0, "append")
            appends += 1
            if appends % args.correction_interval == 0:
                revision += 1
                first = rng.randrange(args.times)
                with state["lock"]:
                    state["targets"][revision] = first
                    state["revision"].value = revision
                commit(first, revision, "replace")
            now = time.monotonic()
            if now >= next_state:
                record_state(args, log, session, "mixed", args.times + appends)
                log("mixed_progress", elapsed_seconds=now - started,
                    appends=appends, replacements=revision, confirmed_batches=confirmed)
                next_state = now + 60
            next_batch += 1
            if now > next_batch:
                log("writer_lag", behind_seconds=now - next_batch)
                next_batch = now
            stop.wait(max(0, min(next_batch, deadline) - time.monotonic()))
        ensure_readers(processes, stop)
        stop.set()
        for process in processes:
            process.join(timeout=300)
            if process.exitcode != 0:
                raise RuntimeError(f"Mixed reader failed: {process.pid}: {process.exitcode}")
        log("mixed_complete", elapsed_seconds=time.monotonic() - started,
            appends=appends, replacements=revision, confirmed_batches=confirmed,
            write_latency_ns=summarize(writes))
        session.owner.checkpoint()
        record_state(args, log, session, "after_mixed_checkpoint", args.times + appends)
        latest_revisions = np.zeros(args.times + appends, dtype=np.int64)
        latest_revisions[:args.times] = state["revisions"][:]
        audit_start = time.monotonic_ns()
        checked = 0
        final_sequence = session.owner.stats()["commit_seq"]
        for stock in range(args.stocks):
            actual = session.read("S", stock, int(workload.keys[0]),
                                  int(workload.keys[args.times + appends - 1]))
            actual.verify(workload.expected("S", stock, 0, args.times + appends, latest_revisions))
            if actual.sequence != final_sequence:
                raise RuntimeError("Final audit snapshot changed after writers stopped")
            checked += len(actual.rows)
            if stock % 100 == 0:
                log("audit_progress", stocks=stock + 1, checked_rows=checked)
        expected = args.stocks * (args.times + appends)
        if checked != expected:
            raise RuntimeError("Final all-field audit row count differs")
        log("audit_complete", checked_rows=checked, final_sequence=final_sequence,
            wall_ns=time.monotonic_ns() - audit_start)
        atomic_json(args.output.parent / "expected-final.json",
                    {"rows": expected, "times": args.times + appends, "stocks": args.stocks,
                     "appends": appends, "replacements": revision, "sequence": final_sequence,
                     "revision_by_time": latest_revisions[:args.times].tolist()})
    finally:
        stop.set()
        start.set()
        for process in processes:
            if process.pid is not None:
                process.join(timeout=10)
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=10)
        session.close()
        ledger.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--stocks", type=int, default=10000)
    parser.add_argument("--times", type=int, default=15661)
    parser.add_argument("--port", type=int, default=18766)
    parser.add_argument("--mode", default="network", choices=("network",))
    parser.add_argument("--duration", type=float, default=86400)
    parser.add_argument("--baseline", type=float, default=60)
    parser.add_argument("--readers", type=int, default=8)
    parser.add_argument("--correction-interval", type=int, default=60)
    args = parser.parse_args()
    if (args.duration < 10 or not 2 <= args.readers <= 8 or args.readers % 2 or
            args.correction_interval < 1):
        parser.error("Use at least 10 seconds, 2/4/6/8 readers, and a positive correction interval")
    log = Events(args.output)
    try:
        mixed(args, log)
        log("complete", status="passed")
    except BaseException as error:
        log("failed", error=repr(error), traceback=traceback.format_exc())
        raise
    finally:
        log.close()


if __name__ == "__main__":
    main()
