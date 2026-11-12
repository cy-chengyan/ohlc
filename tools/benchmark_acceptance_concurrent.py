#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Measure independent readers on the immutable dataset larger than host memory."""

import argparse
import json
import multiprocessing as mp
import os
from pathlib import Path
import random
import statistics
import time
import traceback

from benchmark_acceptance import Events, Session
from benchmark_acceptance_data import FileCache, Workload
from benchmark_metrics import cgroup_swap_snapshot, proc_snapshot, summarize


def read_worker(args, index, kind, count, round_seed, start, ready, window, pipe):
    session = None
    try:
        os.sched_setaffinity(0, {24 + index})
        workload = Workload(args.seed, args.stocks, args.times)
        session = Session(args)
        rng = random.Random(round_seed ^ (index + 1) * 2654435761)
        ready.wait(timeout=300)
        if not start.wait(300):
            raise TimeoutError("Throughput start gate")
        latencies = []
        verify_ns = 0
        prepare_ns = 0
        verified = 0
        while time.monotonic_ns() < window[1]:
            before = time.monotonic_ns()
            first = rng.randrange(args.times - count + 1)
            stock = rng.randrange(args.stocks)
            expected = workload.expected(kind, stock, first, count) if len(latencies) % 16 == 0 else None
            prepare_ns += time.monotonic_ns() - before
            actual = session.read(kind, stock, int(workload.keys[first]),
                                  int(workload.keys[first + count - 1]))
            before = time.monotonic_ns()
            expected_count = args.stocks if kind == "X" else count
            first_key = 0 if kind == "X" else int(workload.keys[first])
            last_key = args.stocks - 1 if kind == "X" else int(workload.keys[first + count - 1])
            if (len(actual.rows) != expected_count or actual.rows[0][0] != first_key or
                    actual.rows[-1][0] != last_key):
                raise RuntimeError("Concurrent query keys or count differ")
            if expected is not None:
                actual.verify(expected)
                verified += 1
            verify_ns += time.monotonic_ns() - before
            latencies.append(actual.wall_ns)
        finished = time.monotonic_ns()
        swap = cgroup_swap_snapshot(os.getpid())
        if swap.get("current_bytes", 0) or swap.get("peak_bytes", 0):
            raise RuntimeError("Concurrent reader swapped")
        pipe.send({"index": index, "latency_ns": latencies, "end_ns": finished,
                   "prepare_ns": prepare_ns, "verify_ns": verify_ns, "verified_queries": verified,
                   "process": proc_snapshot(os.getpid()), "swap": swap})
    except BaseException as error:
        pipe.send({"index": index, "error": repr(error), "traceback": traceback.format_exc()})
    finally:
        if session is not None:
            session.close()
        pipe.close()


def run_round(args, kind, count, readers, seconds, round_seed):
    context = mp.get_context("spawn")
    start = context.Event()
    ready = context.Barrier(readers + 1)
    window = context.Array("q", [0, 0], lock=False)
    pipes = []
    processes = []
    try:
        for index in range(readers):
            receive, send = context.Pipe(duplex=False)
            process = context.Process(target=read_worker,
                                      args=(args, index, kind, count, round_seed,
                                            start, ready, window, send))
            process.start()
            send.close()
            pipes.append(receive)
            processes.append(process)
        ready.wait(timeout=300)
        window[0] = time.monotonic_ns()
        window[1] = window[0] + int(seconds * 1e9)
        start.set()
        results = []
        for pipe in pipes:
            if not pipe.poll(seconds + 300):
                raise TimeoutError("Concurrent reader result")
            result = pipe.recv()
            if "error" in result:
                raise RuntimeError(str(result))
            results.append(result)
        elapsed = (max(item["end_ns"] for item in results) - window[0]) / 1e9
        queries = sum(len(item["latency_ns"]) for item in results)
        return {"seconds": elapsed, "queries": queries, "qps": queries / elapsed,
                "round_seed": round_seed,
                "workers": results}
    finally:
        start.set()
        for process in processes:
            process.join(timeout=10)
            if process.is_alive():
                process.terminate()
                process.join(timeout=10)
        for pipe in pipes:
            pipe.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--stocks", type=int, default=20000)
    parser.add_argument("--times", type=int, default=241920)
    parser.add_argument("--port", type=int, default=18766)
    parser.add_argument("--mode", default="network", choices=("network",))
    parser.add_argument("--smoke", action="store_true")
    args = parser.parse_args()
    log = Events(args.output)
    cache = FileCache(args.data)
    try:
        log("cache_preparation", residency=cache.evict(),
            eviction_scope="Only this test database's data volumes; host-global caches untouched")
        unstable = False
        for kind, count in (("S", 240), ("S", min(4800, args.times)), ("X", 1)):
            for readers in ((1, 4) if args.smoke else (1, 4, 16)):
                for attempt in range(2):
                    rates = []
                    samples = []
                    for round_index in range(4):
                        seconds = .2 if args.smoke else (1 if round_index == 0 else 5)
                        round_seed = (20261111 ^ count * 1009 ^ readers * 6151 ^
                                      attempt * 65537 ^ round_index * 104729)
                        measured = run_round(args, kind, count, readers, seconds, round_seed)
                        log("throughput_round", kind=kind, length=count, readers=readers,
                            attempt=attempt, warmup=round_index == 0, **measured)
                        if round_index:
                            rates.append(measured["qps"])
                            samples.extend(value for worker in measured["workers"]
                                           for value in worker["latency_ns"])
                    cv = statistics.pstdev(rates) / statistics.mean(rates)
                    status = "valid" if cv <= .05 else ("retry" if attempt == 0 else "unstable")
                    log("throughput_summary", kind=kind, length=count, readers=readers,
                        attempt=attempt, mean_qps=statistics.mean(rates), qps=rates,
                        cv=cv, status=status, latency_ns=summarize(samples),
                        residency=cache.residency())
                    if cv <= .05:
                        break
                    unstable |= attempt == 1
        log("complete", status="unstable" if unstable else "passed")
    except BaseException as error:
        log("failed", error=repr(error), traceback=traceback.format_exc())
        raise
    finally:
        cache.close()
        log.close()


if __name__ == "__main__":
    main()
