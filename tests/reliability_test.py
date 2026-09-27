#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Run bounded Linux acceptance cases in fresh, explicitly supplied directories."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import random
import signal
import stat
import struct
import subprocess
import sys
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "clients" / "python"))
from ohlc import Connection, Error, OutcomeUnknown


class Server:
    def __init__(self, binaries, root, *, environment=None, limits=False, network_mib=8):
        self.root = root
        root.mkdir(parents=True, exist_ok=True)
        self.options = dict(socket=str(root / "s"),
                            library=str(binaries / "libohlc_client.so"), timeout_ms=5000)
        self.log_path = root / ("fault.log" if environment else "server.log")
        self.log = self.log_path.open("ab", buffering=0)
        arguments = [str(binaries / "ohlcd"), "--data", str(root / "db"),
                     "--socket", self.options["socket"], "--memory-mib", "128",
                     "--cache-mib", "0"]
        if limits:
            arguments += ["--connections", "4", "--network-mib", str(network_mib),
                          "--timeout-ms", "1000", "--query-ms", "1000"]
        self.process = subprocess.Popen(arguments, stdout=self.log, stderr=self.log,
                                        env=environment)
        try:
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                if self.process.poll() is not None:
                    raise RuntimeError(self.log_path.read_text())
                try:
                    with self.connect():
                        return
                except Error as error:
                    if error.code not in (5, 10):
                        raise
                    time.sleep(0.02)
            raise TimeoutError("Server did not become ready")
        except BaseException:
            self.stop()
            raise

    def connect(self):
        return Connection(**self.options)

    def stop(self):
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGTERM)
            try:
                self.process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
                raise
        self.log.close()
        assert self.process.returncode in (0, 86), self.log_path.read_text()
        if self.process.returncode == 86:
            # The service intentionally refuses to remove a pre-existing socket.
            # Only unlink this fixture's socket after its owning process has exited.
            socket_path = Path(self.options["socket"])
            assert stat.S_ISSOCK(socket_path.lstat().st_mode)
            socket_path.unlink()

    def __enter__(self):
        return self

    def __exit__(self, *exception):
        self.stop()


def values(code, time_key, generation=0):
    if (code + time_key) % 37 == 0:
        return (0,) * 7
    return (-2147483648 + code, 2147483647 - generation, -time_key, generation,
            4294967295 - code, 18446744073709551615 - time_key, 4294967295 - generation)


def write_rows(table, rows):
    sequence = 0
    for start in range(0, len(rows), 2048):
        encoded = bytearray()
        for code, key, row in rows[start:start + 2048]:
            encoded.extend(struct.pack("<IIiiiiIQI", code, key, *row))
        sequence = table.write_encoded(encoded)
    return sequence


def read_rows(query):
    with query:
        chunks = list(query)
    assert chunks and chunks[-1].final
    assert len({chunk.snapshot_sequence for chunk in chunks}) == 1
    return [row for chunk in chunks for row in chunk.rows()]


def verify_oracle(client, definitions, symbols):
    checked = 0
    queries = 0
    for name, oracle in definitions.items():
        table = client.table(name)
        keys = sorted({key for _, key in oracle})
        for code, ticker in symbols.items():
            expected = [(key, *oracle[code, key]) for key in keys if (code, key) in oracle]
            assert read_rows(table.series(ticker, keys[0], keys[-1] + 1)) == expected
            checked += len(expected)
            queries += 1
        absent = [key for key in (keys[0] - 1, keys[-1] + 1) if 0 <= key < 1 << 32]
        for key in keys + absent:
            expected = [(code, *oracle[code, key]) for code in sorted(symbols)
                        if (code, key) in oracle]
            assert read_rows(table.cross(key)) == expected
            checked += len(expected)
            queries += 1
    return checked, queries


def sparse_tables(binaries, root):
    definitions = {}
    symbols = {}
    checked = 0
    queries = 0
    generator = random.Random(20260926)
    with Server(binaries, root) as server:
        with server.connect() as client:
            for index in range(128):
                ticker = f"SPARSE-{index:03d}"
                code, _ = client.register(ticker)
                if index % 2 == 0:
                    symbols[code] = ticker
            for name, period in (("minute", "1m"), ("three_minute", "3m"),
                                 ("day", "1d"), ("five_day", "5d")):
                table = client.create(name, period=period, timezone="UTC" if "m" in period else "")
                base = table.time_key("20260901 09:30:00" if "m" in period else "20260901")
                step = int(period[:-1])
                keys = [base + index * step for index in range(257)]
                oracle = {(code, key): values(code, key) for code in symbols for key in keys
                          if (code * 17 + key) % 5 != 0}
                rows = [(code, key, row) for (code, key), row in oracle.items()]
                generator.shuffle(rows)
                write_rows(table, rows)
                definitions[name] = oracle
            count, total = verify_oracle(client, definitions, symbols)
            checked += count
            queries += total
            for name, oracle in definitions.items():
                changes = []
                for code, key in generator.sample(list(oracle), 200):
                    row = values(code, key, 7)
                    oracle[code, key] = row
                    changes.append((code, key, row))
                first = min(key for _, key in oracle)
                last = max(key for _, key in oracle)
                # Distant and earlier times must not allocate a dense calendar axis.
                for code in symbols:
                    for key in (first - 7, last + 1000000):
                        row = values(code, key, 11)
                        oracle[code, key] = row
                        changes.append((code, key, row))
                generator.shuffle(changes)
                write_rows(client.table(name), changes)
        barrier = threading.Barrier(2)
        def create_same_table(_):
            with server.connect() as client:
                barrier.wait(timeout=5)
                try:
                    client.create("create_race", period="5d")
                    return 0
                except Error as error:
                    return error.code
        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = sorted(pool.map(create_same_table, range(2)))
        assert outcomes == [0, 11], outcomes
    with Server(binaries, root) as server:
        with server.connect() as client:
            count, total = verify_oracle(client, definitions, symbols)
            checked += count
            queries += total
            assert len(list(client.tables())) == 5
    return dict(tables=4, registered_tickers=128, populated_tickers=64,
                checked_rows=checked, queries=queries, create_race=outcomes)


def resources(binaries, root):
    count = 131072
    with Server(binaries, root) as server:
        with server.connect() as client:
            table = client.create("slow", period="1m", timezone="UTC")
            code, _ = client.register("SLOW")
            write_rows(table, [(code, key, values(code, key)) for key in range(count)])
    with Server(binaries, root, limits=True) as server:
        peers = []
        try:
            time.sleep(0.2)
            for _ in range(4):
                peers.append(server.connect())
            try:
                unexpected = server.connect()
            except Error as error:
                cap_error = error.code
                assert cap_error in (5, 10)
            else:
                unexpected.close()
                raise AssertionError("Connection cap was not enforced")
            peers.pop().close()
            time.sleep(0.2)
            peers.append(server.connect())
            peers[-1].ping()
        finally:
            for peer in peers:
                peer.close()
        time.sleep(0.2)
        with server.connect() as slow, server.connect() as fast:
            fast_table = fast.table("slow")
            query = slow.table("slow").series("SLOW", 0, count)
            started = time.monotonic()
            latencies = []
            while time.monotonic() - started < 2:
                before = time.monotonic_ns()
                assert read_rows(fast_table.cross(12345)) == [(code, *values(code, 12345))]
                latencies.append(time.monotonic_ns() - before)
                time.sleep(0.04)
            received = 0
            try:
                for chunk in query:
                    received += chunk.count
            except Error as error:
                slow_error = error.code
                assert slow_error in (5, 10), error
            else:
                raise AssertionError("A stalled reader unexpectedly received a complete stream")
            finally:
                query.close()
            assert received < count
            fast.ping()
            with server.connect() as replacement:
                replacement.ping()
            latencies.sort()
    with Server(binaries, root, limits=True, network_mib=4) as server:
        peers = []
        try:
            time.sleep(0.2)
            # Three response buffers fit; a fourth exceeds the independent byte cap.
            for _ in range(3):
                peers.append(server.connect())
            try:
                unexpected = server.connect()
            except Error as error:
                budget_error = error.code
                assert budget_error in (5, 10)
            else:
                unexpected.close()
                raise AssertionError("Network memory cap was not enforced")
            peers.pop().close()
            time.sleep(0.2)
            peers.append(server.connect())
            peers[-1].ping()
        finally:
            for peer in peers:
                peer.close()
    return dict(connection_cap=4, overflow_status=cap_error, slow_reader_status=slow_error,
                network_mib=4, buffer_cap_status=budget_error, buffer_cap_connections=3,
                slow_partial_rows=received, independent_queries=len(latencies),
                independent_p99_ns=latencies[(99 * len(latencies) + 99) // 100 - 1])


def multi_writer(binaries, root):
    barrier = threading.Barrier(5)
    symbols = {}
    oracle = {}
    queries = 0
    intermediate = 0
    with Server(binaries, root) as server:
        with server.connect() as client:
            table = client.create("writers", period="1m", timezone="UTC")
            for index in range(64):
                ticker = f"WRITER-{index:02d}"
                code, _ = client.register(ticker)
                assert code == index
                symbols[code] = ticker
                for key in range(100):
                    oracle[code, key] = values(code, key, 3)

            def write_partition(partition):
                keys = list(range(100))
                random.Random(partition + 20260926).shuffle(keys)
                sequences = []
                with server.connect() as writer:
                    target = writer.table("writers")
                    barrier.wait(timeout=5)
                    for key in keys:
                        records = [(code, key, oracle[code, key])
                                   for code in range(partition * 16, (partition + 1) * 16)]
                        sequences.append(write_rows(target, records))
                assert sequences == sorted(sequences)
                return sequences

            with ThreadPoolExecutor(max_workers=4) as pool:
                futures = [pool.submit(write_partition, index) for index in range(4)]
                barrier.wait(timeout=5)
                while not all(future.done() for future in futures):
                    key = queries % 100
                    rows = read_rows(table.cross(key))
                    counts = [0] * 4
                    for row in rows:
                        code = row[0]
                        assert row[1:] == oracle[code, key]
                        counts[code // 16] += 1
                    assert all(count in (0, 16) for count in counts), counts
                    intermediate += 0 < len(rows) < 64
                    queries += 1
                sequences = [sequence for future in futures for sequence in future.result()]
            assert len(set(sequences)) == 400
            assert max(sequences) - min(sequences) + 1 == 400
            assert queries > 0 and intermediate > 0
    with Server(binaries, root) as server:
        with server.connect() as client:
            checked, verification_queries = verify_oracle(client, {"writers": oracle}, symbols)
    return dict(writers=4, atomic_batches=400, committed_rows=len(oracle),
                concurrent_snapshot_queries=queries, intermediate_snapshots=intermediate,
                checked_rows=checked, verification_queries=verification_queries)


def crash_boundary(binaries, root, phase):
    symbols = {}
    oracle = {}
    with Server(binaries, root) as server:
        with server.connect() as client:
            client.create("durability", period="1m", timezone="UTC")
            for index in range(32):
                ticker = f"CRASH-{index:02d}"
                code, _ = client.register(ticker)
                symbols[code] = ticker
    arm = root / "armed"
    environment = dict(os.environ, LD_PRELOAD=str(binaries / "fault_inject.so"),
                       OHLC_TEST_CRASH_ON=phase, OHLC_TEST_CRASH_ARM=str(arm),
                       OHLC_TEST_DATABASE=str(root / "db"))
    acknowledged = False
    sequence = None
    with Server(binaries, root, environment=environment) as server:
        with server.connect() as client:
            table = client.table("durability")
            batch = bytearray()
            for code in symbols:
                for key in range(128):
                    row = values(code, key)
                    oracle[code, key] = row
                    batch.extend(struct.pack("<IIiiiiIQI", code, key, *row))
            arm.touch(exist_ok=False)
            try:
                sequence = table.write_encoded(batch)
                acknowledged = True
            except OutcomeUnknown:
                assert phase == "wal"
            assert server.process.wait(timeout=20) == 86
        trace = server.log_path.read_text()
        assert f"crash_boundary={phase} " in trace, trace
        assert acknowledged == (phase != "wal")
    arm.unlink()
    started = time.monotonic_ns()
    with Server(binaries, root) as recovered:
        recovery_ns = time.monotonic_ns() - started
        with recovered.connect() as client:
            assert dict(client.dictionary()) == {code: ticker.encode() for code, ticker in symbols.items()}
            checked, queries = verify_oracle(client, {"durability": oracle}, symbols)
    return dict(boundary=phase, acknowledged=acknowledged, sequence=sequence,
                recovered_rows=len(oracle), checked_rows=checked, queries=queries,
                recovery_ns=recovery_ns, trace=trace.splitlines()[-1])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binaries", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--cases", nargs="+",
                        choices=("sparse_tables", "resources", "multi_writer", "crash"),
                        default=("sparse_tables", "resources", "multi_writer", "crash"))
    args = parser.parse_args()
    binaries = args.binaries.resolve()
    output = args.output.resolve()
    if sys.platform != "linux" or Path("/ssd02") not in output.parents:
        parser.error("Run on Linux with a fresh output directory under /ssd02")
    output.mkdir(mode=0o700)
    with (output / "results.jsonl").open("x", buffering=1) as results:
        cases = []
        if "sparse_tables" in args.cases:
            cases.append(("sparse_tables", lambda: sparse_tables(binaries, output / "sparse")))
        if "resources" in args.cases:
            cases.append(("resources", lambda: resources(binaries, output / "resources")))
        if "multi_writer" in args.cases:
            cases.append(("multi_writer", lambda: multi_writer(binaries, output / "writers")))
        phases = ("wal", "data", "meta", "index", "catalog", "current", "directory")
        for phase in phases if "crash" in args.cases else ():
            cases.append(("crash_" + phase,
                          lambda phase=phase: crash_boundary(binaries, output / phase, phase)))
        for name, operation in cases:
            started = time.monotonic_ns()
            try:
                value = operation()
            except BaseException as error:
                record = dict(case=name, passed=False, error=repr(error),
                              wall_ns=time.monotonic_ns() - started)
                results.write(json.dumps(record, sort_keys=True) + "\n")
                raise
            record = dict(case=name, passed=True, result=value,
                          wall_ns=time.monotonic_ns() - started)
            results.write(json.dumps(record, sort_keys=True) + "\n")
            print(json.dumps(record, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
