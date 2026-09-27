#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Exercise concurrent readers, cache locality and a bounded mixed-write soak."""

import argparse
import ctypes
import json
import os
from pathlib import Path
import queue
import signal
import time

from benchmark_linux import DataCache, Recorder, Worker, disk_usage, proc_snapshot, process_delta, start_server


def stress(worker, recorder, identifier, label, readers, duration_ms, kind="M", length=240,
           warmup=False):
    before = proc_snapshot(worker.engine_pid)
    recorder.progress("stress", label=label, readers=readers, duration_ms=duration_ms,
                      warmup=warmup)
    worker.send(f"B {identifier} {duration_ms} {readers} {kind} {length} 20260926")
    deadline = time.monotonic() + duration_ms / 1000 + 120
    last_progress = time.monotonic()
    while time.monotonic() < deadline:
        try:
            value = worker.events.get(timeout=1)
        except queue.Empty:
            value = None
        if value is not None:
            if value["event"] in ("eof", "reader_error"):
                raise RuntimeError(f"{label}: {value}; inspect {worker.error_file.name}")
            if value["event"] == "writer_complete":
                worker.writer_done = True
            if value["event"] == "stress_complete":
                assert value["id"] == identifier and not value["sample_limit"], value
                assert value["queries"] == value["series"]["queries"] + value["cross"]["queries"]
                assert value["queries"] > 0
                value["qps"] = value["queries"] * 1e9 / value["wall_ns"]
                value["rows_per_second"] = value["rows"] * 1e9 / value["wall_ns"]
                after = proc_snapshot(worker.engine_pid)
                recorder.record("stress_result", label=label, value=value, warmup=warmup,
                                engine_process=after, process_io=process_delta(before, after))
                print(json.dumps({"event": "stress_result", "label": label,
                                  "qps": value["qps"], "series": value["series"],
                                  "cross": value["cross"], "warmup": warmup}), flush=True)
                return value
            recorder.record("worker_event", label=label, value=value)
        recorder.record("process_sample", label=label, value=proc_snapshot(worker.engine_pid))
        if time.monotonic() - last_progress >= 15:
            recorder.progress("stress", label=label, readers=readers, running=True)
            last_progress = time.monotonic()
    raise TimeoutError(label)


def warm_data(database, recorder):
    started = time.monotonic_ns()
    total = 0
    for path in sorted((database / "tables").glob("*/data-*.dat")):
        if path.is_symlink() or database not in path.resolve().parents:
            raise RuntimeError("Unexpected data volume")
        with path.open("rb", buffering=0) as stream:
            while True:
                block = stream.read(8 << 20)
                if not block:
                    break
                total += len(block)
    cache = DataCache(database)
    resident = 0
    pages = 0
    try:
        for _, size, address, vector in cache.files:
            if cache.libc.mincore(address, size, vector) != 0:
                raise OSError(ctypes.get_errno(), "mincore")
            values = vector.raw
            resident += sum(value & 1 for value in values)
            pages += len(values)
    finally:
        cache.close()
    assert resident == pages, (resident, pages)
    recorder.record("warm_data", bytes=total, pages=pages, resident_pages=resident,
                    wall_ns=time.monotonic_ns() - started)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("database", type=Path)
    parser.add_argument("--write-start", type=int, default=65584)
    parser.add_argument("--soak-seconds", type=int, default=300)
    args = parser.parse_args()
    root = args.root.resolve()
    database = args.database.resolve()
    if Path("/ssd02") not in root.parents or Path("/ssd02") not in database.parents:
        parser.error("All run directories must be under /ssd02")
    if not 30 <= args.soak_seconds <= 600:
        parser.error("Use a bounded 30..600 second soak")
    output = root / "acceptance-results"
    output.mkdir(mode=0o700)
    recorder = Recorder(output)
    available = set(os.sched_getaffinity(0))
    engine_cpus = set(range(16))
    client_cpus = set(range(24, 40))
    if not (engine_cpus | client_cpus) <= available:
        raise RuntimeError("This run requires the recorded two-socket CPU layout")
    recorder.record("configuration", stocks=10000, history_times=65536,
                    engine_cpus=sorted(engine_cpus), client_cpus=sorted(client_cpus),
                    write_start=args.write_start, soak_seconds=args.soak_seconds,
                    database=str(database), disk=disk_usage(database))
    identifier = 0
    try:
        for cache_mib in (None, 0, 64):
            transport = "embedded" if cache_mib is None else "tcp"
            label = transport if cache_mib is None else f"tcp-cache{cache_mib}"
            server = None
            log = None
            endpoint = database
            os.sched_setaffinity(0, engine_cpus)
            if cache_mib is not None:
                server, log, endpoint = start_server(root, database, recorder, "tcp", cache_mib, label)
                os.sched_setaffinity(0, client_cpus)
            worker = Worker(root / "ohlc_workload", endpoint, 10000, 65536, recorder,
                            label, server.pid if server else None)
            os.sched_setaffinity(0, client_cpus)
            try:
                warm_data(database, recorder)
                reader_counts = (1, 4, 8, 16) if cache_mib != 64 else (1, 8)
                for readers in reader_counts:
                    for warmup, milliseconds in ((True, 3000), (False, 20000)):
                        identifier += 1
                        stress(worker, recorder, identifier, f"{label}/uniform/{readers}",
                               readers, milliseconds, warmup=warmup)
                if cache_mib is None:
                    for kind, case in (("S", "series"), ("X", "cross")):
                        identifier += 1
                        stress(worker, recorder, identifier, f"{label}/{case}/1", 1, 15000, kind)
                if cache_mib is not None:
                    # Restrict only generated query keys; the database remains unchanged.
                    worker.close()
                    worker = Worker(root / "ohlc_workload", endpoint, 10000, 128, recorder,
                                    label + "-window", server.pid)
                    identifier += 1
                    stress(worker, recorder, identifier, f"{label}/window128/8", 8, 3000,
                           length=60, warmup=True)
                    identifier += 1
                    stress(worker, recorder, identifier, f"{label}/window128/8", 8, 20000, length=60)
                if cache_mib == 64:
                    worker.close()
                    worker = Worker(root / "ohlc_workload", endpoint, 10000, 65536, recorder,
                                    "soak", server.pid)
                    recorder.progress("soak", seconds=args.soak_seconds, rows_per_second=10000)
                    worker.send(f"W {args.write_start} {args.soak_seconds} 1000 0")
                    identifier += 1
                    stress(worker, recorder, identifier, "tcp-cache64/soak/8", 8,
                           args.soak_seconds * 1000)
                    if not worker.writer_done:
                        worker.wait("writer_complete")
            finally:
                worker.close()
                if server is not None:
                    server.send_signal(signal.SIGTERM)
                    server.wait(timeout=120)
                    log.close()
                    assert server.returncode == 0
        os.sched_setaffinity(0, engine_cpus)
        end = args.write_start + args.soak_seconds
        worker = Worker(root / "ohlc_workload", database, 10000, end, recorder, "post-soak")
        try:
            worker.query(0, "S", 9999, args.write_start, args.soak_seconds, mode="post-soak")
            worker.query(1, "X", 0, end - 1, 1, mode="post-soak")
        finally:
            worker.close()
        recorder.progress("complete", final_times=end, disk=disk_usage(database))
    except Exception as error:
        recorder.progress("failed", error=repr(error))
        raise
    finally:
        os.sched_setaffinity(0, available)
        recorder.output.close()


if __name__ == "__main__":
    main()
