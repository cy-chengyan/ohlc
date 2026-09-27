#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Run bounded Linux workloads with verified, file-specific cold-cache reads."""

import argparse
import ctypes
import datetime
import json
import math
import os
from pathlib import Path
import queue
import random
import signal
import socket
import subprocess
import sys
import threading
import time


GIB = 1 << 30
PAGE_SIZE = os.sysconf("SC_PAGE_SIZE")
TICKS = os.sysconf("SC_CLK_TCK")


def proc_snapshot(pid):
    """Read process counters without inserting work into the engine's locks."""
    try:
        root = Path("/proc") / str(pid)
        fields = (root / "stat").read_text().rsplit(") ", 1)[1].split()
        counters = {}
        for line in (root / "io").read_text().splitlines():
            name, value = line.split(":", 1)
            counters[name] = int(value)
        counters["cpu_ns"] = (int(fields[11]) + int(fields[12])) * 1_000_000_000 // TICKS
        counters["rss_bytes"] = int(fields[21]) * PAGE_SIZE
        for line in (root / "status").read_text().splitlines():
            if line.startswith("VmHWM:"):
                counters["peak_rss_bytes"] = int(line.split()[1]) * 1024
        return counters
    except (FileNotFoundError, ProcessLookupError, PermissionError) as error:
        # Linux may revoke /proc/<pid>/io access while a process becomes a zombie.
        return {"counters_unavailable": type(error).__name__}


def process_delta(before, after):
    return {key: after[key] - before[key]
            for key in ("read_bytes", "write_bytes", "rchar", "wchar", "cpu_ns")
            if key in before and key in after}


class Recorder:
    def __init__(self, root, resume=False):
        self.root = root
        self.output = (root / "results.jsonl").open("a" if resume else "x", buffering=1)
        summary = root / "summary.json"
        self.groups = json.loads(summary.read_text()) if resume and summary.exists() else []

    def record(self, event, **fields):
        value = {"observed_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                 "event": event, **fields}
        self.output.write(json.dumps(value, sort_keys=True) + "\n")
        return value

    def progress(self, phase, **fields):
        value = self.record("progress", phase=phase, **fields)
        temporary = self.root / "status.tmp"
        temporary.write_text(json.dumps(value, indent=2) + "\n")
        temporary.replace(self.root / "status.json")
        print(json.dumps(value, sort_keys=True), flush=True)

    def summary(self, label, rows):
        def percentiles(key):
            values = sorted(row[key] for row in rows)
            return {name: values[max(0, math.ceil(fraction * len(values)) - 1)]
                    for name, fraction in (("p50", .5), ("p95", .95), ("p99", .99))}

        io_samples = [row for row in rows if "read_bytes" in row["process_io"]]
        embedded = label.startswith("embedded/")
        result = {"label": label, "samples": len(rows), "rows_per_query": rows[0]["rows"],
                  "attempt": rows[0].get("attempt"),
                  "wall_ns": percentiles("wall_ns"),
                  "verify_ns": percentiles("verify_ns"),
                  "thread_cpu_ns": percentiles("thread_cpu_ns"),
                  "mean_engine_read_bytes": (sum(r["engine_read_bytes"] for r in rows) / len(rows)
                                             if embedded else None),
                  "mean_engine_read_calls": (sum(r["engine_read_calls"] for r in rows) / len(rows)
                                             if embedded else None),
                  "process_io_samples": len(io_samples),
                  "total_process_read_bytes": (sum(r["process_io"]["read_bytes"] for r in io_samples)
                                               if len(io_samples) == len(rows) else None),
                  "max_rss_bytes": max(r["engine_process"].get("rss_bytes", 0) for r in rows)}
        self.groups.append(result)
        self.record("summary", **result)
        (self.root / "summary.json").write_text(json.dumps(self.groups, indent=2) + "\n")
        print(json.dumps({"event": "summary", **result}, sort_keys=True), flush=True)


class Worker:
    def __init__(self, binary, endpoint, stocks, times, recorder, label, engine_pid=None):
        self.recorder = recorder
        self.label = label
        self.events = queue.Queue()
        self.writer_done = False
        self.error_file = (recorder.root / (label + ".stderr")).open("a")
        self.process = subprocess.Popen([str(binary), "read", str(endpoint), str(stocks), str(times)],
                                        stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=self.error_file, text=True, bufsize=1)
        self.engine_pid = engine_pid or self.process.pid
        self.reader = threading.Thread(target=self._read, daemon=True)
        self.reader.start()
        self.wait("ready")

    def _read(self):
        try:
            for line in self.process.stdout:
                self.events.put(json.loads(line))
        except Exception as error:
            self.events.put({"event": "reader_error", "error": repr(error)})
        finally:
            self.events.put({"event": "eof"})

    def wait(self, event, identifier=None):
        deadline = time.monotonic() + 330
        while time.monotonic() < deadline:
            value = self.events.get(timeout=max(.1, deadline - time.monotonic()))
            if value["event"] in ("eof", "reader_error"):
                raise RuntimeError(f"{self.label}: {value}; inspect {self.error_file.name}")
            if value["event"] == "writer_complete":
                self.writer_done = True
            if value["event"] == event and (identifier is None or value.get("id") == identifier):
                if event != "query":
                    self.recorder.record("worker_event", label=self.label, value=value)
                return value
            self.recorder.record("worker_event", label=self.label, value=value)
        raise TimeoutError(self.label)

    def send(self, command):
        self.process.stdin.write(command + "\n")
        self.process.stdin.flush()

    def query(self, identifier, kind, stock, first, length, **context):
        before = proc_snapshot(self.engine_pid)
        self.send(f"{kind} {identifier} {stock} {first} {length}")
        value = self.wait("query", identifier)
        after = proc_snapshot(self.engine_pid)
        value.update(context)
        value["label"] = self.label
        value["process_io"] = process_delta(before, after)
        value["engine_process"] = after
        self.recorder.record("sample", value=value)
        return value

    def close(self):
        if self.process.poll() is None:
            self.send("STOP")
        try:
            code = self.process.wait(timeout=120)
        except subprocess.TimeoutExpired:
            self.process.terminate()
            self.process.wait(timeout=30)
            raise
        self.reader.join(timeout=5)
        self.process.stdin.close()
        self.process.stdout.close()
        self.error_file.close()
        if code != 0:
            raise RuntimeError(f"{self.label} exited with status {code}")


class DataCache:
    """Evict only clean data pages belonging to this benchmark database."""
    def __init__(self, database):
        self.libc = ctypes.CDLL(None, use_errno=True)
        self.libc.mmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int,
                                  ctypes.c_int, ctypes.c_int, ctypes.c_long]
        self.libc.mmap.restype = ctypes.c_void_p
        self.libc.mincore.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p]
        self.libc.munmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
        self.files = []
        for path in sorted((database / "tables").glob("*/data-*.dat")):
            if path.is_symlink() or not path.is_file() or database not in path.resolve().parents:
                raise RuntimeError("Unexpected data path")
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
            size = os.fstat(fd).st_size
            pages = (size + PAGE_SIZE - 1) // PAGE_SIZE
            address = self.libc.mmap(None, size, 0, 1, fd, 0)
            if address == ctypes.c_void_p(-1).value:
                os.close(fd)
                raise OSError(ctypes.get_errno(), "mmap")
            self.files.append((fd, size, address, ctypes.create_string_buffer(pages)))
        if not self.files:
            raise RuntimeError("No data volumes to evict")

    def evict(self):
        started = time.monotonic_ns()
        # Readahead from the preceding query may still be in flight when the
        # first eviction runs. Retry with a bound, but never label warm pages cold.
        bit_zero = bytes([value & 1 for value in range(256)])
        for attempt in range(10):
            resident = 0
            total = 0
            for fd, size, address, vector in self.files:
                os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
                if self.libc.mincore(address, size, vector) != 0:
                    raise OSError(ctypes.get_errno(), "mincore")
                data = vector.raw
                resident += len(data) - data.translate(bit_zero).count(0)
                total += len(data)
            if resident == 0:
                return {"resident_pages_before": 0, "total_data_pages": total,
                        "eviction_attempts": attempt + 1,
                        "eviction_ns": time.monotonic_ns() - started}
            time.sleep(.02 * (attempt + 1))
        raise RuntimeError(f"Cold-cache verification failed: {resident}/{total} pages remain")

    def close(self):
        for fd, size, address, _ in self.files:
            self.libc.munmap(address, size)
            os.close(fd)


def disk_usage(root):
    result = {"logical_bytes": 0, "allocated_bytes": 0, "by_kind": {}}
    for directory, directories, files in os.walk(root):
        for name in directories + files:
            if (Path(directory) / name).is_symlink():
                raise RuntimeError("Symlinks are not allowed inside the benchmark")
        for name in files:
            info = (Path(directory) / name).stat()
            result["logical_bytes"] += info.st_size
            result["allocated_bytes"] += info.st_blocks * 512
            kind = name.split("-", 1)[0] if "-" in name else name
            result["by_kind"][kind] = result["by_kind"].get(kind, 0) + info.st_size
    space = os.statvfs(root)
    result["filesystem_free_bytes"] = space.f_bavail * space.f_frsize
    if result["allocated_bytes"] > 80 * GIB or result["filesystem_free_bytes"] < 64 * GIB:
        raise RuntimeError("Benchmark disk budget reached")
    return result


def load_stage(binary, database, stocks, first, end, recorder):
    recorder.progress("load", first=first, times=end, disk=disk_usage(recorder.root.parent))
    with (recorder.root / f"load-{end}.stderr").open("x") as errors:
        process = subprocess.Popen([str(binary), "load", str(database), str(stocks),
                                    str(first), str(end)], stdout=subprocess.PIPE, stderr=errors,
                                   text=True, bufsize=1)
        for line in process.stdout:
            value = json.loads(line)
            recorder.record("load_event", value=value, process=proc_snapshot(process.pid))
            if value["event"] in ("registered", "load_progress", "load_complete"):
                recorder.progress("load", target=end, value=value)
        process.stdout.close()
        if process.wait() != 0:
            raise RuntimeError(f"Load failed; inspect load-{end}.stderr")
    recorder.record("capacity", times=end, database=disk_usage(database))


def run_reads(binary, endpoint, database, stocks, times, recorder, transport,
              samples, cache_modes, engine_pid=None):
    worker = Worker(binary, endpoint, stocks, times, recorder, f"{transport}-{times}", engine_pid)
    cache = DataCache(database)
    identifier = 0
    cases = [("series-60", "S", 60), ("series-240", "S", 240)]
    if times > 4096:
        cases.append(("series-4096", "S", 4096))
    cases += [("series-full", "S", times), ("cross", "X", 1)]
    try:
        for mode in cache_modes:
            for name, kind, length in cases:
                label = f"{transport}/{times}/{mode}/{name}"
                if any(group["label"] == label for group in recorder.groups):
                    continue
                attempt = time.time_ns()
                recorder.progress("read", label=label, samples=samples, attempt=attempt)
                generator = random.Random(20260926 + times + length)
                rows = []
                for _ in range(samples):
                    stock = generator.randrange(stocks)
                    first = generator.randrange(times - length + 1)
                    if mode == "hot":
                        worker.query(identifier, kind, stock, first, length,
                                     mode="warmup", case=name, attempt=attempt)
                        identifier += 1
                        eviction = {}
                    else:
                        eviction = cache.evict()
                    rows.append(worker.query(identifier, kind, stock, first, length,
                                             mode=mode, case=name, cache=eviction, attempt=attempt))
                    identifier += 1
                recorder.summary(label, rows)
    finally:
        cache.close()
        worker.close()


def start_server(root, database, recorder, transport, cache_mib=0, label=None):
    command = [str(root / "ohlcd"), "--data", str(database), "--memory-mib", "4096",
               "--cache-mib", str(cache_mib), "--query-ms", "300000", "--timeout-ms", "300000"]
    if transport == "unix":
        address = root / "benchmark.sock"
        command += ["--socket", str(address)]
        endpoint = "unix:" + str(address)
    else:
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        command += ["--host", "127.0.0.1", "--port", str(port)]
        endpoint = f"tcp:{port}"
    log = (recorder.root / f"server-{label or transport}.log").open("x")
    process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        if process.poll() is not None:
            log.close()
            raise RuntimeError(f"Server exited: {command}")
        try:
            family = socket.AF_UNIX if transport == "unix" else socket.AF_INET
            with socket.socket(family) as probe:
                probe.settimeout(.5)
                probe.connect(str(address) if transport == "unix" else ("127.0.0.1", port))
            recorder.record("server", transport=transport, pid=process.pid, command=command)
            return process, log, endpoint
        except (FileNotFoundError, ConnectionRefusedError, socket.timeout):
            time.sleep(.1)
    process.terminate()
    process.wait(timeout=30)
    log.close()
    raise TimeoutError("Server startup")


def run_mixed(binary, endpoint, stocks, times, first_write, recorder, transport, engine_pid=None):
    worker = Worker(binary, endpoint, stocks, times, recorder, f"mixed-{transport}", engine_pid)
    generator = random.Random(20260926)
    rows = {"series-240": [], "cross": []}
    try:
        cases = (("series-240", "S", 240), ("cross", "X", 1))
        working_set = []
        identifier = 0
        for _ in range(24):
            for name, kind, length in cases:
                first = generator.randrange(times - length + 1)
                stock = generator.randrange(stocks)
                working_set.append((name, kind, stock, first, length))
                worker.query(identifier, kind, stock, first, length, mode="mixed-warmup", case=name)
                identifier += 1
        baseline = {"series-240": [], "cross": []}
        for index in range(200):
            name, kind, stock, first, length = working_set[index % len(working_set)]
            baseline[name].append(worker.query(identifier, kind, stock, first, length,
                                                mode="mixed-idle", case=name))
            identifier += 1
        for name, samples in baseline.items():
            recorder.summary(f"{transport}/{times}/mixed-idle/{name}", samples)
        recorder.progress("mixed", transport=transport, batches=24, rows_per_batch=stocks,
                          interval_ms=1000)
        worker.send(f"W {first_write} 24 1000 16")
        started = time.monotonic()
        index = 0
        while time.monotonic() - started < 24.5:
            for _ in range(2):
                name, kind, stock, first, length = working_set[index % len(working_set)]
                rows[name].append(worker.query(identifier, kind, stock, first, length,
                                               mode="mixed", case=name))
                identifier += 1
                index += 1
            # Bound sample volume while the writer runs at the specified rate.
            time.sleep(.04)
        if not worker.writer_done:
            worker.wait("writer_complete")
        for name, samples in rows.items():
            recorder.summary(f"{transport}/{times}/mixed/{name}", samples)
    finally:
        worker.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--stocks", type=int, default=10000)
    parser.add_argument("--stages", type=int, nargs="+", default=[4096, 16384, 65536])
    parser.add_argument("--samples", type=int, default=100)
    parser.add_argument("--resume-from", type=int, default=0,
                        help="Reuse a completed import stage and resume its unfinished read groups")
    args = parser.parse_args()
    if sys.platform != "linux":
        parser.error("This driver requires Linux process counters, mincore and posix_fadvise")
    root = args.root.absolute()
    if root.resolve() != root or Path("/ssd02") not in root.parents or not root.is_dir():
        parser.error("Use an existing, directly owned directory beneath /ssd02")
    if root.stat().st_uid != os.getuid():
        parser.error("The run directory must belong to the current user")
    if (args.stocks < 1 or args.stocks > 20000 or args.samples < 1 or
            args.stages != sorted(set(args.stages)) or min(args.stages) < 240 or
            max(args.stages) > 1000000):
        parser.error("Invalid workload dimensions")
    database = root / "database"
    if database.exists() and args.resume_from == 0:
        parser.error("A fresh database path is required")
    if args.resume_from and (args.resume_from not in args.stages or not database.is_dir()):
        parser.error("Resume requires an existing database and a configured stage boundary")
    results = root / "results"
    results.mkdir(mode=0o700, exist_ok=bool(args.resume_from))
    recorder = Recorder(results, bool(args.resume_from))
    binary = root / "ohlc_workload"
    try:
        recorder.record("environment", platform=os.uname()._asdict() if hasattr(os.uname(), "_asdict")
                        else list(os.uname()), cpuinfo=Path("/proc/cpuinfo").read_text().split("\n\n")[0],
                        meminfo=Path("/proc/meminfo").read_text(),
                        arguments={"root": str(root), "stocks": args.stocks,
                                   "stages": args.stages, "samples": args.samples},
                        disk=disk_usage(root), page_size=PAGE_SIZE, clock_ticks=TICKS)
        first = args.resume_from
        for end in args.stages:
            if end < first:
                continue
            if end > first:
                load_stage(binary, database, args.stocks, first, end, recorder)
            else:
                recorder.record("resumed_import", times=end, database=disk_usage(database))
            run_reads(binary, database, database, args.stocks, end, recorder, "embedded",
                      args.samples, ["hot", "cold"])
            first = end
        for transport in ("unix", "tcp"):
            server, log, endpoint = start_server(root, database, recorder, transport)
            try:
                run_reads(binary, endpoint, database, args.stocks, first, recorder, transport,
                          args.samples, ["hot"] if transport == "unix" else ["hot", "cold"],
                          server.pid)
                if transport == "tcp":
                    run_mixed(binary, endpoint, args.stocks, first, first, recorder,
                              transport, server.pid)
            finally:
                server.send_signal(signal.SIGTERM)
                server.wait(timeout=120)
                log.close()
                if server.returncode != 0:
                    raise RuntimeError("Server failed during shutdown")
        run_mixed(binary, database, args.stocks, first, first + 24, recorder, "embedded")
        recorder.progress("complete", stocks=args.stocks, history_times=first,
                          live_times=48, database=disk_usage(database))
    except Exception as error:
        recorder.progress("failed", error=repr(error))
        raise
    finally:
        recorder.output.close()


if __name__ == "__main__":
    main()
