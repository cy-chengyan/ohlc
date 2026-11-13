#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Optional competitor drivers; no competitor package enters the project build."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import ctypes
import multiprocessing
import queue
import traceback
import http.client
import json
import os
from pathlib import Path
import random
import secrets
import statistics
import struct
import subprocess
import sys
import time
import threading
import urllib.parse

from benchmark_influx import settled_state
from benchmark_metrics import (LATENCY_CAP_SECONDS, THROUGHPUT_ROUND_SECONDS,
                               THROUGHPUT_WARMUP_SECONDS, proc_snapshot, process_delta,
                               sha256, summarize, disk_usage, cgroup_swap_snapshot)

ROW = struct.Struct("<IqqqqQQQiI")
WRITE = struct.Struct("<IIqqqqQQQiI")
FIELDS = "open,high,low,close,volume,amount,adjust_factor,reserved,flags"


@dataclass(frozen=True)
class ExpectedQuery:
    count: int
    first_key: int
    last_key: int
    encoded: bytes | None = None


@dataclass
class DecodedRows:
    rows: list
    encoded: bytes | None = None
    chunks: list | None = None

    def verify(self, expected):
        # Both paths have already decoded every integer during execute_query.
        encoded = self.encoded
        if self.chunks is not None:
            encoded = b"".join(self.chunks)
        matches = (encoded == expected if encoded is not None else
                   self.rows == list(ROW.iter_unpack(expected)))
        if not matches:
            raise RuntimeError("Field/flags/key/order/count mismatch")

    def verify_throughput(self, expected):
        if len(self.rows) != expected.count or (self.rows and (
                self.rows[0][0] != expected.first_key or self.rows[-1][0] != expected.last_key)):
            raise RuntimeError("Query count or boundary key mismatch")
        if expected.encoded is not None:
            self.verify(expected.encoded)



class Oracle:
    def __init__(self, stocks, times):
        script = Path(__file__).with_name("benchmark_quotation.py")
        self.process = subprocess.Popen(
            [sys.executable, str(script), "oracle", str(stocks), str(times)],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE)

    def request_raw(self, kind, stock, first, length):
        self.process.stdin.write(f"{kind} {stock} {first} {length}\n".encode())
        self.process.stdin.flush()
        header = self.process.stdout.read(4)
        if len(header) != 4:
            raise RuntimeError("Generator stopped")
        count = struct.unpack("<I", header)[0]
        if kind == "T":
            return count
        data = self.process.stdout.read(count * ROW.size)
        if len(data) != count * ROW.size:
            raise RuntimeError("Truncated oracle result")
        return data

    def request(self, kind, stock, first, length):
        value = self.request_raw(kind, stock, first, length)
        return value if kind == "T" else list(ROW.iter_unpack(value))

    def close(self):
        self.process.stdin.close()
        self.process.stdout.close()
        if self.process.wait() != 0:
            raise RuntimeError("Generator failed")


class HttpDriver:
    def __init__(self, port):
        self.http = http.client.HTTPConnection("127.0.0.1", port, timeout=300)
        self.token = None

    def prepare_call(self, path, body=b"", headers=None, method="POST"):
        headers = dict(headers or {})
        if self.token:
            headers["Authorization"] = "Token " + self.token
        return method, path, body, headers

    def execute_call(self, prepared):
        method, path, body, headers = prepared
        self.http.request(method, path, body, headers)
        response = self.http.getresponse()
        data = response.read()
        if response.status >= 300:
            # Do not echo response bodies: setup responses may contain credentials.
            raise RuntimeError(f"HTTP status {response.status} on {path.split('?')[0]}")
        return data

    def call(self, path, body=b"", headers=None, method="POST"):
        return self.execute_call(self.prepare_call(path, body, headers, method))

    def close(self):
        self.http.close()


class Ohlc:
    def __init__(self, binary, stocks, port):
        sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "clients/python"))
        from ohlc import Connection
        self.client = Connection(host="127.0.0.1", port=port, timeout_ms=300000,
                                 library=str(binary.parent / "libohlc_client.so"))
        self.stocks = stocks
        self.table = None

    def create(self):
        self.table = self.client.create("comparison", period="1m", timezone="Asia/Shanghai")
        for index in range(self.stocks):
            code, _ = self.table.register(f"S{index:05d}")
            if code != index:
                raise RuntimeError("Comparison dictionary order")

    def prepare_write(self, encoded):
        self.open_table()
        storage = (ctypes.c_ubyte * len(encoded)).from_buffer_copy(encoded)
        return storage, len(encoded) // WRITE.size, ctypes.c_uint64()

    def execute_write(self, prepared):
        storage, count, sequence = prepared
        with self.client._lock:
            self.client._check(self.client._lib.ohlc_client_write(
                self.client._handle, self.table.id, storage, count, ctypes.byref(sequence)))

    def open_table(self):
        if self.table is None:
            self.table = self.client.table("comparison")

    def settle(self):
        self.client.checkpoint()

    def prepare_query(self, kind, stock, first_key, last_key):
        from ohlc import _Query
        self.open_table()
        # Dictionary IDs were checked during import; no resolve RPC belongs in a query.
        arguments = ((self.client._handle, self.table.id, first_key) if kind == "X" else
                     (self.client._handle, self.table.id, stock, first_key, last_key))
        function = (self.client._lib.ohlc_client_cross if kind == "X" else
                    self.client._lib.ohlc_client_series)
        return function, arguments, _Query(self.client)

    def execute_query(self, prepared):
        function, arguments, query = prepared
        with self.client._lock:
            self.client._check(function(*arguments))
        result = []
        chunks = []
        with query:
            for chunk in query:
                result.extend(ROW.iter_unpack(chunk.data))
                chunks.append(chunk.data)
        return DecodedRows(result, chunks=chunks)

    def close(self):
        self.client.close()


class ClickHouse(HttpDriver):
    def __init__(self):
        super().__init__(18123)

    def sql(self, query, data=b""):
        return self.call("/?query=" + urllib.parse.quote(query), data)

    def create(self):
        types = ("UInt32", "UInt32", "Int64", "Int64", "Int64", "Int64", "UInt64",
                 "UInt64", "UInt64", "Int32", "UInt32")
        names = ("ticker,time," + FIELDS).split(",")
        columns = ",".join(f"{name} {kind} CODEC(LZ4)" for name, kind in zip(names, types))
        self.sql("CREATE TABLE bars (" + columns +
                 ",PROJECTION cross_order (SELECT * ORDER BY (time,ticker))) "
                 "ENGINE=MergeTree ORDER BY (ticker,time) "
                 "SETTINGS fsync_after_insert=1,fsync_part_directory=1")

    def prepare_write(self, encoded):
        return self.prepare_call("/?query=" + urllib.parse.quote(
            "INSERT INTO bars FORMAT RowBinary"), encoded)

    def execute_write(self, prepared):
        self.execute_call(prepared)

    def settle(self):
        self.sql("OPTIMIZE TABLE bars FINAL")
        while int(self.sql("SELECT count() FROM system.merges FORMAT TabSeparated")):
            time.sleep(1)

    def prepare_query(self, kind, stock, first_key, last_key):
        sql = self.query_sql(kind, stock, first_key, last_key)
        return self.prepare_call("/?query=" + urllib.parse.quote(sql + " FORMAT RowBinary"))

    def query_sql(self, kind, stock, first_key, last_key):
        predicate = f"time={first_key}" if kind == "X" else (
            f"ticker={stock} AND time>={first_key} AND time<={last_key}")
        key = "ticker" if kind == "X" else "time"
        settings = " SETTINGS optimize_read_in_order=0" if kind == "X" else ""
        return f"SELECT {key},{FIELDS} FROM bars WHERE {predicate} ORDER BY {key}{settings}"

    def probe_plans(self, keys, times, require_projection):
        plans = []
        for position in (0, times // 2, times - 1):
            sql = self.query_sql("X", 0, keys[position], keys[position])
            plan = self.sql("EXPLAIN projections=1,indexes=1 " + sql).decode()
            if require_projection and "ReadFromMergeTree (cross_order)" not in plan:
                raise RuntimeError("ClickHouse cross-section does not read the configured projection")
            plans.append({"position": position, "sql": sql, "plan": plan})
        return plans

    def execute_query(self, prepared):
        data = self.execute_call(prepared)
        if len(data) % ROW.size:
            raise RuntimeError("Truncated ClickHouse row")
        return DecodedRows(list(ROW.iter_unpack(data)), data)


class InfluxDB(HttpDriver):
    def __init__(self, token_path, setup=False, data=None):
        super().__init__(18086)
        self.sort_required = {"S": True, "X": True}
        self.token_path = token_path
        self.data = data
        if setup:
            self.token = secrets.token_urlsafe(32)
            payload = json.dumps({"username": "bench", "password": secrets.token_urlsafe(32),
                                  "org": "bench", "bucket": "bench", "token": self.token})
            self.call("/api/v2/setup", payload, {"Content-Type": "application/json"})
            descriptor = os.open(token_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "w") as output:
                output.write(self.token)
        else:
            self.token = token_path.read_text()

    def create(self):
        buckets = json.loads(self.call("/api/v2/buckets?name=bench", method="GET"))["buckets"]
        bucket = buckets[0]
        payload = json.dumps({"bucketID": bucket["id"], "database": "bench", "default": True,
                              "orgID": bucket["orgID"], "retention_policy": "autogen"})
        self.call("/api/v2/dbrps", payload, {"Content-Type": "application/json"})

    def prepare_write(self, encoded):
        lines = []
        names = FIELDS.split(",")
        for record in WRITE.iter_unpack(encoded):
            ticker, key, *values = record
            fields = []
            for index, (name, value) in enumerate(zip(names, values)):
                if index == 8 or values[-1] & (1 << index):
                    suffix = "u" if index in (4, 5, 6) else "i"
                    fields.append(f"{name}={value}{suffix}")
            lines.append(f"bars,ticker=S{ticker:05d} {','.join(fields)} {key * 60}")
        return self.prepare_call("/api/v2/write?org=bench&bucket=bench&precision=s",
                                 "\n".join(lines).encode())

    def execute_write(self, prepared):
        self.execute_call(prepared)

    def settle(self):
        deadline = time.monotonic() + 3600
        quiet = None
        previous = None
        while time.monotonic() < deadline:
            raw = self.call("/metrics", method="GET").decode()
            state = settled_state(self.data, raw)
            if state is not None:
                if state != previous:
                    quiet = time.monotonic()
                # Cover the configured 60-second full-compaction cold interval and scheduler.
                if time.monotonic() - quiet >= 65:
                    return {**state, "raw": raw}
            else:
                quiet = None
            previous = state
            time.sleep(1)
        raise TimeoutError("InfluxDB cache snapshot/compaction did not finish")

    def prepare_query(self, kind, stock, first_key, last_key):
        predicate = f"time={first_key * 60000000000}" if kind == "X" else (
            f"ticker='S{stock:05d}' AND time>={first_key * 60000000000} "
            f"AND time<={last_key * 60000000000}")
        query = f"SELECT ticker,{FIELDS} FROM bars WHERE {predicate}"
        return kind, self.prepare_call("/query?db=bench&epoch=s&q=" + urllib.parse.quote(query),
                                       method="GET")

    def execute_query(self, prepared):
        kind, request = prepared
        response = json.loads(self.execute_call(request))
        result = []
        for group in response["results"]:
            if "error" in group:
                raise RuntimeError("InfluxQL query error")
            for series in group.get("series", []):
                columns = series["columns"]
                key_index = columns.index("ticker" if kind == "X" else "time")
                field_indexes = [columns.index(name) for name in FIELDS.split(",")]
                for values in series["values"]:
                    key = int(values[key_index][1:]) if kind == "X" else values[key_index] // 60
                    result.append((key, *(0 if values[index] is None else values[index]
                                          for index in field_indexes)))
        if self.sort_required[kind]:
            result.sort(key=lambda row: row[0])
        return DecodedRows(result)

    def probe_order(self, oracle, keys, stocks, times):
        observations = []
        for kind in ("S", "X"):
            for first in (0, times // 2, times - 1):
                count = min(480, times - first) if kind == "S" else 1
                stock = (first + 17) % stocks
                prepared = self.prepare_query(kind, stock, keys[first], keys[first + count - 1])
                actual = self.execute_query(prepared)
                ordered = all(left[0] < right[0] for left, right in zip(actual.rows, actual.rows[1:]))
                observations.append({"kind": kind, "first": first, "count": len(actual.rows),
                                     "server_ordered": ordered})
                if not ordered:
                    actual.rows.sort(key=lambda row: row[0])
                actual.verify(oracle.request_raw(kind, stock, first, count))
            self.sort_required[kind] = True
        return {"observations": observations, "sort_required": self.sort_required.copy()}


class Aerospike:
    def __init__(self, stocks, keys):
        import aerospike
        from aerospike_helpers.batch import records
        from aerospike_helpers.operations import bitwise_operations, operations
        self.client = aerospike.client({"hosts": [("127.0.0.1", 13000)],
                                       "policies": {"read": {"max_retries": 0},
                                                    "write": {"max_retries": 0}}}).connect()
        self.records = records
        self.bits = bitwise_operations
        self.ops = operations
        self.stocks = stocks
        self.keys = keys
        self.positions = {key: index for index, key in enumerate(keys)}

    def create(self):
        # The isolated server configuration creates the namespace before connecting.
        return None

    def prepare_write(self, encoded):
        changes = {}
        for offset in range(0, len(encoded), WRITE.size):
            ticker, key = struct.unpack_from("<II", encoded, offset)
            position = self.positions[key]
            day, minute = divmod(position, 240)
            item = ("bench", "bars", f"S{ticker:05d}:{day}")
            if item not in changes:
                changes[item] = []
            # A whole-day import initializes a blob. Realtime updates use bit_set.
            changes[item].append((minute, encoded[offset + 8:offset + WRITE.size]))
        requests = []
        for key, entries in changes.items():
            if entries[0][0] == 0:
                blob = bytearray(30 + 240 * 64)
                for minute, value in entries:
                    blob[minute // 8] |= 1 << (7 - minute % 8)
                    blob[30 + minute * 64:30 + (minute + 1) * 64] = value
                operations = [self.ops.write("rows", blob)]
            else:
                operations = []
                for minute, value in entries:
                    operations += [self.bits.bit_set("rows", minute, 1, 1, bytearray([128])),
                                   self.bits.bit_set("rows", (30 + minute * 64) * 8, 512, 64,
                                                     bytearray(value))]
            requests.append(self.records.Write(key, operations))
        return [self.records.BatchRecords(requests[first:first + 1000])
                for first in range(0, len(requests), 1000)]

    def execute_write(self, prepared):
        for request in prepared:
            result = self.client.batch_write(request)
            for record in result.batch_records:
                if record.result != 0:
                    raise RuntimeError(f"Aerospike write status {record.result}")

    def settle(self):
        deadline = time.monotonic() + 3600
        quiet = None
        while time.monotonic() < deadline:
            observed = {}
            for node, response in self.client.info_all("namespace/bench").items():
                values = dict(field.split("=", 1) for field in response[1].split(";") if "=" in field)
                queues = {key: int(value) for key, value in values.items()
                          if key.startswith("storage-engine.") and
                          key.endswith((".write_q", ".defrag_q"))}
                if not any(key.endswith(".write_q") for key in queues):
                    raise RuntimeError("Aerospike storage queue metrics unavailable")
                observed[node] = queues
            if observed and all(value == 0 for queues in observed.values() for value in queues.values()):
                quiet = quiet or time.monotonic()
                if time.monotonic() - quiet >= 5:
                    return {"storage_queues": observed, "durability_equivalent": False}
            else:
                quiet = None
            time.sleep(1)
        raise TimeoutError("Aerospike write/defrag queues did not settle")

    def prepare_query(self, kind, stock, first_key, last_key):
        first = self.positions[first_key]
        last = self.positions[last_key]
        requests = []
        keys = []
        if kind == "X":
            targets = [(ticker, first, first) for ticker in range(self.stocks)]
        else:
            # A nonaligned first day needs the next request at the following day boundary.
            targets = []
            position = first
            while position <= last:
                end = min(last, position // 240 * 240 + 239)
                targets.append((stock, position, end))
                position = end + 1
        for ticker, start, end in targets:
            day, minute = divmod(start, 240)
            key = ("bench", "bars", f"S{ticker:05d}:{day}")
            requests.append(self.records.Read(key, [self.bits.bit_get(
                "rows", (30 + minute * 64) * 8, (end - start + 1) * 512)]))
            keys.append((ticker, start, end))
        return kind, self.records.BatchRecords(requests), keys

    def execute_query(self, prepared):
        kind, request, keys = prepared
        response = self.client.batch_write(request)
        if len(response.batch_records) != len(keys):
            raise RuntimeError("Aerospike batch result count")
        result = []
        for record, (ticker, start, end) in zip(response.batch_records, keys):
            if record.result != 0:
                raise RuntimeError(f"Aerospike read status {record.result}")
            blob = record.record[2]["rows"]
            if len(blob) != (end - start + 1) * 64:
                raise RuntimeError("Aerospike bit slice length")
            for offset, position in enumerate(range(start, end + 1)):
                values = struct.unpack_from("<qqqqQQQiI", blob, offset * 64)
                result.append((ticker if kind == "X" else self.keys[position], *values))
        return DecodedRows(result)

    def close(self):
        self.client.close()


def make_driver(args, keys, setup=False):
    if args.system == "ohlc":
        return Ohlc(args.binary, args.stocks, args.port)
    if args.system == "clickhouse":
        return ClickHouse()
    if args.system == "influxdb":
        driver = InfluxDB(args.token, setup, args.data)
        driver.sort_required = getattr(args, "influx_sort_required", driver.sort_required)
        return driver
    return Aerospike(args.stocks, keys)


class PreparedOracle:
    """Bounded lookahead preserves the deterministic uniform request sequence."""

    def __init__(self, args, keys, kind, index):
        self.keys = keys
        self.oracle = Oracle(args.stocks, args.times)
        self.items = queue.Queue(maxsize=8)
        self.stop = threading.Event()
        self.rng = random.Random(20260926 ^ (index + 1) * 2654435761)
        self.args = args
        self.kind = kind
        self.thread = threading.Thread(target=self.produce, daemon=True)
        self.thread.start()

    def produce(self):
        sequence = 0
        try:
            while not self.stop.is_set():
                first = self.rng.randrange(self.args.times if self.kind == "X" else
                                           self.args.times - 240 + 1)
                stock = self.rng.randrange(self.args.stocks)
                count = 1 if self.kind == "X" else 240
                # Metadata uses the same oracle-generated time dictionary as the requests.
                expected = ExpectedQuery(
                    self.args.stocks if self.kind == "X" else count,
                    0 if self.kind == "X" else self.keys[first],
                    self.args.stocks - 1 if self.kind == "X" else self.keys[first + count - 1],
                    self.oracle.request_raw(self.kind, stock, first, count)
                    if sequence % 16 == 0 else None)
                self.put((stock, first, expected))
                sequence += 1
        except BaseException as error:
            self.put(error)

    def put(self, value):
        while not self.stop.is_set():
            try:
                self.items.put(value, timeout=.1)
                return
            except queue.Full:
                continue

    def get(self):
        item = self.items.get(timeout=300)
        if isinstance(item, BaseException):
            raise item
        return item

    def close(self):
        self.stop.set()
        self.thread.join(timeout=10)
        if self.thread.is_alive():
            self.oracle.process.kill()
            self.thread.join(timeout=10)
            raise RuntimeError("Oracle producer did not stop")
        self.oracle.close()


def throughput_reader(args, keys, kind, index, ready, start_event, window, pipe):
    connection = None
    expected_rows = None
    try:
        connection = make_driver(args, keys)
        expected_rows = PreparedOracle(args, keys, kind, index)
        pending = expected_rows.get()
        if isinstance(connection, HttpDriver):
            connection.http.connect()
        elif isinstance(connection, Ohlc):
            connection.open_table()
        before = proc_snapshot(os.getpid())
        oracle_before = proc_snapshot(expected_rows.oracle.process.pid)
        ready.wait(timeout=300)
        if not start_event.wait(timeout=300):
            raise TimeoutError("Reader start gate")
        latencies = []
        verify_ns = 0
        full_verify_queries = 0
        prepare_ns = 0
        oracle_wait_ns = 0
        while time.monotonic_ns() < window[1]:
            if pending is None:
                started = time.monotonic_ns()
                pending = expected_rows.get()
                oracle_wait_ns += time.monotonic_ns() - started
            stock, first, expected = pending
            pending = None
            started = time.monotonic_ns()
            last = first if kind == "X" else first + 239
            request = connection.prepare_query(kind, stock, keys[first], keys[last])
            prepare_ns += time.monotonic_ns() - started
            started = time.monotonic_ns()
            actual = connection.execute_query(request)
            elapsed = time.monotonic_ns() - started
            started = time.monotonic_ns()
            actual.verify_throughput(expected)
            verify_ns += time.monotonic_ns() - started
            full_verify_queries += int(expected.encoded is not None)
            latencies.append(elapsed)
        finished = time.monotonic_ns()
        after = proc_snapshot(os.getpid())
        oracle_after = proc_snapshot(expected_rows.oracle.process.pid)
        pipe.send({"latency_ns": latencies, "finished_ns": finished, "verify_ns": verify_ns,
                   "full_verify_queries": full_verify_queries,
                   "prepare_ns": prepare_ns, "oracle_wait_ns": oracle_wait_ns,
                   "process": after, "process_delta": process_delta(before, after),
                   "oracle_process": oracle_after,
                   "oracle_delta": process_delta(oracle_before, oracle_after)})
    except BaseException as error:
        ready.abort()
        pipe.send({"error": type(error).__name__, "traceback": traceback.format_exc()})
    finally:
        try:
            if expected_rows is not None:
                expected_rows.close()
            if connection is not None:
                connection.close()
        finally:
            pipe.close()


def throughput_round(args, keys, kind, readers, duration):
    context = multiprocessing.get_context("spawn")
    ready = context.Barrier(readers + 1)
    start_event = context.Event()
    window = context.Array("Q", 2, lock=False)
    processes = []
    pipes = []
    try:
        for index in range(readers):
            parent, child = context.Pipe(duplex=False)
            process = context.Process(target=throughput_reader,
                                      args=(args, keys, kind, index, ready, start_event, window, child))
            process.start()
            child.close()
            processes.append(process)
            pipes.append(parent)
        ready.wait(timeout=300)
        window[0] = time.monotonic_ns()
        window[1] = window[0] + int(duration * 1e9)
        start_event.set()
        results = []
        for pipe in pipes:
            if not pipe.poll(duration + 300):
                raise TimeoutError("Throughput reader response")
            value = pipe.recv()
            if "error" in value:
                raise RuntimeError("Throughput reader failed: " + value["traceback"])
            results.append(value)
        for process in processes:
            process.join(timeout=30)
            if process.exitcode != 0:
                raise RuntimeError("Throughput reader cleanup failed")
        seconds = (max(item["finished_ns"] for item in results) - window[0]) / 1e9
        reader_wall_ns = sum(item["finished_ns"] - window[0] for item in results)
        totals = {name: sum(item[name] for item in results)
                  for name in ("verify_ns", "prepare_ns", "oracle_wait_ns", "full_verify_queries")}
        return {"latency_ns": [item.pop("latency_ns") for item in results],
                "seconds": seconds, "reader_wall_ns": reader_wall_ns, **totals,
                "verify_fraction": totals["verify_ns"] / reader_wall_ns,
                "prepare_fraction": totals["prepare_ns"] / reader_wall_ns,
                "oracle_wait_fraction": totals["oracle_wait_ns"] / reader_wall_ns,
                "reader_processes": results}
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
            process.join(timeout=10)
        for pipe in pipes:
            pipe.close()


def run_realtime_batches(args, driver, oracle, keys, record):
    realtime_batches = 2 if args.smoke else 301
    realtime_latencies = []
    for index in range(realtime_batches):
        expected = oracle.request("X", 0, args.times + index, 1)
        encoded = b"".join(WRITE.pack(row[0], keys[args.times + index], *row[1:]) for row in expected)
        prepared = driver.prepare_write(encoded)
        started = time.monotonic_ns()
        driver.execute_write(prepared)
        wall_ns = time.monotonic_ns() - started
        realtime_latencies.append(wall_ns)
        prepared = driver.prepare_query("X", 0, keys[args.times + index], keys[args.times + index])
        driver.execute_query(prepared).verify(b"".join(ROW.pack(*row) for row in expected))
        record("realtime", rows=args.stocks, wall_ns=wall_ns,
               durability_equivalent=args.system != "aerospike")
        time.sleep(max(0, 1 - (time.monotonic_ns() - started) / 1e9))
    record("realtime_summary", wall_ns=summarize(realtime_latencies),
           durability_equivalent=args.system != "aerospike")


def run_driver(args):
    output = args.output.open("w", buffering=1)

    def record(event, **values):
        output.write(json.dumps({"event": event, **values}) + "\n")

    realtime_batches = 2 if args.smoke else 301
    oracle = Oracle(args.stocks, args.times + realtime_batches)
    keys = [oracle.request("T", 0, index, 1) for index in range(args.times + realtime_batches)]
    driver = make_driver(args, keys, True)
    if args.system == "aerospike":
        import aerospike
        import importlib.metadata
        record("client_library", version=importlib.metadata.version("aerospike"),
               path=aerospike.__file__, sha256=sha256(aerospike.__file__))

    def snapshot():
        result = {"client": proc_snapshot(os.getpid()), "server": proc_snapshot(args.server_pid),
                  "driver_cgroup": cgroup_swap_snapshot(os.getpid())}
        if args.system == "ohlc":
            result["engine"] = driver.client.stats()
        return result

    def group_end(label, before):
        after = snapshot()
        if any(item["driver_cgroup"].get(name, 0) for item in (before, after)
               for name in ("current_bytes", "peak_bytes")):
            raise RuntimeError("Competitor driver cgroup swapped")
        record("group_counters", label=label, before=before, after=after,
               client_delta=process_delta(before["client"], after["client"]),
               server_delta=process_delta(before["server"], after["server"]),
               engine_delta={key: value - before.get("engine", {}).get(key, 0)
                             for key, value in after.get("engine", {}).items()})

    start = time.monotonic()
    import_before = snapshot()
    def import_timeout():
        record("invalid", stage="import_or_consolidation", reason="60-minute import limit")
        output.flush()
        # Exit only this isolated driver unit; systemd reaps its generator children.
        os._exit(124)

    timer = threading.Timer(3600, import_timeout)
    timer.daemon = True
    timer.start()
    generator = None
    stage = "import"
    write_ns = 0
    imported_rows = 0
    prepare_ns = 0
    try:
        driver.create()
        script = Path(__file__).with_name("benchmark_quotation.py")
        generator = subprocess.Popen(
            [sys.executable, str(script), "export", str(args.stocks), str(args.times)],
            stdout=subprocess.PIPE)
        batch_times = 240 if args.system == "aerospike" else 8
        record("import_model", batch_times=batch_times, stocks=args.stocks,
               durability_equivalent=args.system != "aerospike")
        while True:
            encoded = generator.stdout.read(args.stocks * batch_times * WRITE.size)
            if not encoded:
                break
            before = time.monotonic_ns()
            prepared = driver.prepare_write(encoded)
            prepared_ns = time.monotonic_ns() - before
            before = time.monotonic_ns()
            driver.execute_write(prepared)
            elapsed = time.monotonic_ns() - before
            write_ns += elapsed
            prepare_ns += prepared_ns
            imported_rows += len(encoded) // WRITE.size
            record("import_batch", rows=len(encoded) // WRITE.size,
                   wall_ns=elapsed, prepare_ns=prepared_ns)
            if time.monotonic() - start > 3600:
                raise TimeoutError("Import exceeded 60 minutes")
        if generator.wait() != 0:
            raise RuntimeError("Import generator failed")
        imported = time.monotonic()
        stage = "consolidation"
        settle_start = time.monotonic_ns()
        settle = driver.settle()
        settle_ns = time.monotonic_ns() - settle_start
        timer.cancel()
        if time.monotonic() - start > 3600:
            raise TimeoutError("Import including consolidation exceeded 60 minutes")
        record("import_complete", import_seconds=imported - start,
               settled_seconds=time.monotonic() - start, settle_observation=settle,
               compaction_verified=True, write_ns=write_ns, settle_ns=settle_ns,
               prepare_ns=prepare_ns, rows=imported_rows,
               rows_per_second=imported_rows * 1e9 / (write_ns + settle_ns),
               compression={"ohlc": "none", "clickhouse": "LZ4 columns and projection",
                            "influxdb": "native TSM encodings",
                            "aerospike": "uncompressed application blobs"}[args.system])
        record("space_after_import", value=disk_usage(args.data),
               records=args.stocks * args.times)
        group_end("import", import_before)
        unstable = False
        stage = "reads"
        if isinstance(driver, ClickHouse):
            record("query_plans", plans=driver.probe_plans(keys, args.times, not args.smoke))
        if isinstance(driver, InfluxDB):
            record("ordering_probe", **driver.probe_order(oracle, keys, args.stocks, args.times))
            args.influx_sort_required = driver.sort_required.copy()
        for length in (1, 60, 240, 4800, args.times, 0):
            latency_before = snapshot()
            length = min(length, args.times)
            randomizer = random.Random(20260926 + max(1, length))
            values = []
            begin = time.monotonic()
            for sample in range(3 if args.smoke else 1000):
                kind = "X" if length == 0 else "S"
                count = max(1, length)
                first = randomizer.randrange(args.times - count + 1)
                stock = randomizer.randrange(args.stocks)
                expected = oracle.request_raw(kind, stock, first, count)
                prepared = driver.prepare_query(kind, stock, keys[first], keys[first + count - 1])
                driver.execute_query(prepared).verify(expected)
                prepare_start = time.monotonic_ns()
                prepared = driver.prepare_query(kind, stock, keys[first], keys[first + count - 1])
                preparation = time.monotonic_ns() - prepare_start
                started = time.monotonic_ns()
                actual = driver.execute_query(prepared)
                wall = time.monotonic_ns() - started
                started = time.monotonic_ns()
                actual.verify(expected)
                verify_ns = time.monotonic_ns() - started
                values.append(wall)
                record("query", kind=kind, length=length, sample=sample, rows=len(actual.rows),
                       wall_ns=wall, verify_ns=verify_ns, prepare_ns=preparation)
                if sample >= 199 and time.monotonic() - begin > LATENCY_CAP_SECONDS:
                    break
            record("latency_summary", length=length, wall_ns=summarize(values),
                   warmup="One fully verified identical query before each timed query")
            group_end(f"latency-{length}", latency_before)
        for kind in ("S", "X"):
            for readers in (1, 4, 16):
                for attempt in range(2):
                    rates = []
                    for round_index in range(4):
                        duration = (0.2 if round_index == 0 else 1) if args.smoke else (
                            THROUGHPUT_WARMUP_SECONDS if round_index == 0 else
                            THROUGHPUT_ROUND_SECONDS)
                        throughput_before = snapshot()
                        measured = throughput_round(args, keys, kind, readers, duration)
                        qps = sum(map(len, measured["latency_ns"])) / measured["seconds"]
                        record("throughput_round", readers=readers, kind=kind, attempt=attempt,
                               warmup=round_index == 0, qps=qps, **measured)
                        group_end(f"throughput-{kind}-{readers}-{attempt}-{round_index}", throughput_before)
                        if round_index:
                            rates.append(qps)
                    cv = statistics.pstdev(rates) / statistics.mean(rates)
                    record("throughput_summary", readers=readers, kind=kind, attempt=attempt, cv=cv,
                           validity=("unstable" if attempt else "retry") if cv > .05 else "valid")
                    unstable |= attempt == 1 and cv > .05
                    if cv <= .05:
                        break
        # Generate fresh complete-time batches, preserving the same values as ohlc.
        stage = "realtime"
        if isinstance(driver, HttpDriver):
            # The control connection was idle while independent throughput readers ran.
            # Reopen it before timing any mutation; never retry an ambiguous write.
            driver.http.close()
            driver.http.connect()
        realtime_before = snapshot()
        run_realtime_batches(args, driver, oracle, keys, record)
        group_end("realtime", realtime_before)
        record("post_realtime_consolidation", value=driver.settle())
        record("space_after_realtime", value=disk_usage(args.data),
               records=args.stocks * (args.times + realtime_batches))
        record("complete", controlled_cold="incomparable", process=proc_snapshot(os.getpid()),
               validity="unstable" if unstable else "valid")
    except Exception as error:
        record("invalid", stage=stage, reason=repr(error))
        raise
    finally:
        timer.cancel()
        if generator is not None:
            if generator.poll() is None:
                generator.terminate()
            generator.stdout.close()
            generator.wait()
        driver.close()
        oracle.close()
        output.close()


def run_realtime_only(args):
    """Resume independent write measurements after a failed read group, without reimporting."""
    if args.system != "influxdb":
        raise ValueError("Realtime recovery is currently supported only for InfluxDB")
    realtime_batches = 2 if args.smoke else 301
    oracle = Oracle(args.stocks, args.times + realtime_batches)
    keys = [oracle.request("T", 0, index, 1) for index in range(args.times + realtime_batches)]
    driver = make_driver(args, keys, False)
    with args.output.open("x", buffering=1) as output:
        def record(event, **values):
            output.write(json.dumps({"event": event, **values}) + "\n")

        stage = "recovery_validation"
        try:
            query = "/query?db=bench&q=" + urllib.parse.quote("SELECT count(flags) FROM bars")
            counted = json.loads(driver.call(query, method="GET"))
            if any("error" in item for item in counted["results"]):
                raise RuntimeError("InfluxDB recovered count query failed")
            count = sum(row[1] for group in counted["results"]
                        for series in group.get("series", []) for row in series["values"])
            if count != args.stocks * args.times:
                raise RuntimeError(f"Recovered row count differs from the unmodified baseline: {count}")
            probes = driver.probe_order(oracle, keys, args.stocks, args.times)
            first_new = driver.prepare_query("X", 0, keys[args.times], keys[args.times])
            if driver.execute_query(first_new).rows:
                raise RuntimeError("The realtime target already contains rows")
            record("recovery_validation", rows=count, ordering=probes,
                   scope="Realtime only; the earlier OOM read group remains failed")
            before = {"client": proc_snapshot(os.getpid()), "server": proc_snapshot(args.server_pid),
                      "driver_cgroup": cgroup_swap_snapshot(os.getpid())}
            stage = "realtime"
            run_realtime_batches(args, driver, oracle, keys, record)
            after = {"client": proc_snapshot(os.getpid()), "server": proc_snapshot(args.server_pid),
                     "driver_cgroup": cgroup_swap_snapshot(os.getpid())}
            if any(item["driver_cgroup"].get(key, 0) for item in (before, after)
                   for key in ("current_bytes", "peak_bytes")):
                raise RuntimeError("Realtime driver cgroup swapped")
            record("group_counters", label="realtime", before=before, after=after,
                   client_delta=process_delta(before["client"], after["client"]),
                   server_delta=process_delta(before["server"], after["server"]))
            record("post_realtime_consolidation", value=driver.settle())
            record("space_after_realtime", value=disk_usage(args.data),
                   records=args.stocks * (args.times + realtime_batches))
            record("complete", validity="valid", scope="realtime_only_after_read_oom",
                   controlled_cold="incomparable", process=proc_snapshot(os.getpid()))
        except Exception as error:
            record("invalid", stage=stage, reason=repr(error))
            raise
        finally:
            driver.close()
            oracle.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--system", choices=("ohlc", "clickhouse", "influxdb", "aerospike"), required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--port", type=int, default=18765)
    parser.add_argument("--server-pid", type=int, required=True)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--stocks", type=int, required=True)
    parser.add_argument("--times", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--token", type=Path, required=True)
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--realtime-only", action="store_true")
    arguments = parser.parse_args()
    if arguments.realtime_only:
        run_realtime_only(arguments)
    else:
        run_driver(arguments)
