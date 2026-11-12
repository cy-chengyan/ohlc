# SPDX-License-Identifier: Apache-2.0
"""Exercise the actual server, C client binding, framing and durable mutations."""

import datetime
from concurrent.futures import ThreadPoolExecutor
import os
import json
from pathlib import Path
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "clients" / "python"))
from ohlc import Connection, Error
from series_contract import check_closed_series
from nullable_contract import check_nullable
from shell_edit_test import Terminal


def require_error(code, operation):
    try:
        operation()
    except Error as error:
        assert error.code == code, error
    else:
        raise AssertionError(f"Expected status {code}")


def connect_when_ready(process, **options):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(process.stderr.read().decode())
        try:
            return Connection(**options)
        except Error as error:
            if error.code not in (5, 10):
                raise
        time.sleep(0.02)
    raise TimeoutError("Server did not become ready")


def check_memory_pressure(server, library):
    """Exercise server-side backpressure waiting at a fixed small budget."""
    with tempfile.TemporaryDirectory(prefix="ohlc-net-memory-",
                                     dir=os.environ.get("TMPDIR", "/tmp")) as directory:
        root = Path(directory)
        options = dict(socket=str(root / "socket"), library=library)
        arguments = [server, "--data", str(root / "db"), "--socket", options["socket"],
                     "--memory-mib", "2", "--cache-mib", "0"]
        process = subprocess.Popen(arguments, stderr=subprocess.PIPE)
        batches = 200

        def write(first):
            with Connection(**options) as client:
                table = client.table("pressure")
                for batch in range(batches):
                    records = bytearray()
                    for time_index in range(8):
                        key = 18000 + batch * 8 + time_index
                        for ticker in range(first, first + 16):
                            records.extend(struct.pack("<IIqqqqQQQiI", ticker, key,
                                                       0, 0, 0, key, 0, 0, 0, 0, 8))
                    sequence = table.write_encoded(records)
                    if batch % 16 == 0:
                        assert client.stats()["memory_bytes"] <= 2 * 1024 * 1024
                return sequence

        try:
            with connect_when_ready(process, **options) as client:
                table = client.create("pressure", period="1d")
                for ticker in range(64):
                    assert client.register(table.id, f"S{ticker}")[0] == ticker
                initial = client.stats()["checkpoint_seq"]
                with ThreadPoolExecutor(max_workers=4) as workers:
                    sequences = list(workers.map(write, range(0, 64, 16)))
                stats = client.stats()
                assert stats["commit_seq"] == max(sequences)
                assert stats["checkpoint_seq"] > initial
                client.checkpoint()
                assert client.stats()["checkpoint_seq"] == max(sequences)
        finally:
            process.send_signal(signal.SIGTERM)
            _, errors = process.communicate(timeout=20)
            assert process.returncode == 0, errors.decode()
            assert b"checkpoint: resource limit" not in errors, errors.decode()
        process = subprocess.Popen(arguments, stderr=subprocess.PIPE)
        try:
            with connect_when_ready(process, **options) as client:
                assert client.stats()["commit_seq"] == max(sequences)
                table = client.table("pressure")
                for ticker in range(0, 64, 16):
                    with table.series(f"S{ticker}", 18000, 18000 + batches * 8 - 1) as query:
                        rows = [row for chunk in query for row in chunk.rows()]
                    assert rows == [(key, None, None, None, key, None, None, None, None)
                                    for key in range(18000, 18000 + batches * 8)]
                assert client.stats()["memory_bytes"] <= 2 * 1024 * 1024
        finally:
            process.send_signal(signal.SIGTERM)
            _, errors = process.communicate(timeout=20)
            assert process.returncode == 0, errors.decode()


def check_immediate_busy(server, library):
    """Keep a checkpoint blocked until a disabled-wait request returns BUSY."""
    shim = Path(server).resolve().parent / "fault_inject.so"
    if not sys.platform.startswith("linux") or not shim.exists():
        return
    with tempfile.TemporaryDirectory(prefix="ohlc-busy-", dir="/tmp") as directory:
        root = Path(directory)
        gate = root / "gate"
        os.mkfifo(gate)
        release = os.open(gate, os.O_RDWR | os.O_NONBLOCK)
        environment = dict(os.environ, LD_PRELOAD=str(shim), OHLC_TEST_CHECKPOINT_GATE=str(gate))
        options = dict(socket=str(root / "socket"), library=library)
        process = subprocess.Popen([server, "--data", str(root / "db"),
                                    "--socket", options["socket"], "--memory-mib", "2",
                                    "--cache-mib", "0", "--busy-wait-ms", "0"],
                                   env=environment, stderr=subprocess.PIPE)
        try:
            with connect_when_ready(process, **options) as client:
                table = client.create("pressure", period="1d")
                client.register(table.id, "S0")
                for batch in range(10000):
                    record = struct.pack("<IIqqqqQQQiI", 0, batch * 8,
                                         0, 0, 0, batch, 0, 0, 0, 0, 8)
                    try:
                        table.write_encoded(record)
                    except Error as error:
                        assert error.code == 4
                        break
                else:
                    raise AssertionError("Small budget did not produce backpressure")
                # The checkpoint stays blocked until after this response. No
                # elapsed-time assertion or scheduling race determines success.
                before = client.stats()["commit_seq"]
                try:
                    table.write_encoded(record)
                except Error as error:
                    assert error.code == 4
                else:
                    raise AssertionError("Expected immediate BUSY")
                assert client.stats()["commit_seq"] == before
        finally:
            os.write(release, b"x")
            process.send_signal(signal.SIGTERM)
            _, errors = process.communicate(timeout=20)
            os.close(release)
            assert process.returncode == 0, errors.decode()


def check_shell_queries(execute, options):
    """Check inclusive shell bounds and server-side ticker-set parsing."""
    assert " protocol=6 " in execute("ping; status;").stderr
    values = (1, 2, 0, 1, 3, 4, 1000000)
    with Connection(**options) as client:
        minute = client.create("shell_range", period="3m", timezone="Asia/Shanghai")
        day = client.create("shell_dates", period="5d")
        minute.write([("AAPL", f"20260901 {time}", values)
                      for time in ("09:30:00", "09:31:00", "09:33:00", "09:34:00")])
        minute.insert("GOOGL", "20260901 09:30:00", values)
        minute.insert("INTL", "20260901 09:33:00", values)
        for name in (b",", b")", b"A,B", b"x\0"):
            minute.insert(name, "20260901 09:30:00", values)
        day.write([("AAPL", date, values) for date in ("20260901", "20260902", "20260906")])
        day.insert("ONLY_OTHER_TABLE", "20260901", values)
        day.insert("MAX", 0xffffffff, values)
        original = client.stats()

        def keys(command):
            output = execute(command + " --format jsonl;").stdout
            return [json.loads(line)["key"] for line in output.splitlines()]

        start = 'series shell_range AAPL from "20260901 09:30:00"'
        assert keys(start + ' and "20260901 09:33:00" --all') == [
            minute.time_key(f"20260901 {time}") for time in ("09:30:00", "09:31:00", "09:33:00")]
        assert keys(start + ' and "20260901 09:30:00"') == [minute.time_key("20260901 09:30:00")]
        assert keys('series shell_dates AAPL from "20260901" and "20260902"') == [
            day.time_key("20260901"), day.time_key("20260902")]
        assert keys("series shell_dates MAX from @4294967295 and @4294967295") == [0xffffffff]
        execute(start + ' to "20260901 09:33:00";', 2)
        execute(start + ' and "20260901 09:29:00";', 1)
        execute("series shell_dates MAX from @4294967295 and @4294967296;", 1)

        prefix = 'cross shell_range "20260901 09:30:00"'
        wanted = [minute.resolve("AAPL"), minute.resolve("GOOGL")]
        assert keys(prefix + " and ticker in ('GOOGL','UNKNOWN','AAPL','AAPL','INTL','ONLY_OTHER_TABLE')") == wanted
        assert keys(prefix + " AnD TiCkEr In(GOOGL,AAPL)") == wanted
        assert keys(prefix + " and ticker in ()") == []
        assert keys(prefix + " and ticker in ('UNKNOWN')") == []
        assert keys(prefix + " and ticker in (" + ",".join(["'AAPL'"] * 100) + ")") == wanted[:1]
        assert keys(prefix + r" and ticker in (',',')','A,B','x\x00')") == [
            minute.resolve(name) for name in (b",", b")", b"A,B", b"x\0")]
        # Malformed lists are syntax errors (exit 2); an empty name is a value error.
        for clause in ("and ticker in ('AAPL',)", "and ticker in ('AAPL' 'GOOGL')",
                       "and ticker in ('AAPL'", "and ticker in (,AAPL)",
                       "and ticker in ('AAPL'))"):
            execute(prefix + " " + clause + ";", 2)
        execute(prefix + " and ticker in ('');", 1)
        assert len(keys(prefix)) == 6
        current = client.stats()
        assert current["ticker_count"] == original["ticker_count"]
        assert current["commit_seq"] == original["commit_seq"]
        client.drop("shell_range")
        client.drop("shell_dates")


def check_extended_periods(execute, options):
    definitions = (("seconds", "5s", ' --timezone Asia/Shanghai', "20260901 09:30:17", 1788226217),
                   ("months", "3mo", "", "20260917", 20713),
                   ("years", "2y", "", "20260917", 20713))
    with Connection(**options) as client:
        for name, period, zone, stamp, key in definitions:
            execute(f"create {name} --period {period}{zone};")
            execute(f'insert {name} AAPL "{stamp}" 1 2 0 1 3 4 1000000;')
            table = client.table(name)
            assert table.period == period and table.time_key(stamp) == key
            table.insert("AAPL", key + 1, (1, 2, 0, 1, 3, 4, 1000000))
            query = f'series {name} AAPL from "{stamp}" and @{key + 1} --all --format jsonl;'
            assert [json.loads(line)["key"] for line in execute(query).stdout.splitlines()] == [key, key + 1]
            result = execute(f'cross {name} "{stamp}" and ticker in (AAPL, UNKNOWN) --format jsonl;')
            assert [json.loads(line)["key"] for line in result.stdout.splitlines()] == [0]
            assert f"\t{name}\t{period}\t" in execute(f"describe {name};").stdout
            check_closed_series(table, key, stamp, (1, 2, 0, 1, 3, 4, 1000000))
            assert [json.loads(line)["key"] for line in execute(query).stdout.splitlines()] == [key, key + 1]
        execute('insert seconds MAX "2106-02-07T06:28:15Z" 1 2 0 1 3 4 1000000;')
        result = execute('series seconds MAX from @4294967295 and @4294967295 --format jsonl;')
        assert json.loads(result.stdout)["key"] == 0xffffffff
        for stamp in ("2106-02-07T06:28:16Z", "1969-12-31T23:59:59Z",
                      "20260901 09:30:60", "20260901 09:30:00.1"):
            execute(f'cross seconds "{stamp}";', 1)
        for period in ("0s", "1M", "1h", "4294967296y"):
            execute(f"create invalid --period {period};", 1)
        execute("create no_zone --period 1s;", 1)
        for period in ("1s", "2mo", "3y"):
            table = client.create("python_period", period=period,
                                  timezone="UTC" if period.endswith("s") else "")
            assert table.period == period
            client.drop("python_period")
        for name, *_ in definitions:
            client.drop(name)


def check_stale_table(shell_arguments, options):
    """Re-open a cached table name after another client drops and re-creates it."""
    process = subprocess.Popen(shell_arguments + ["--file", "-"], stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    def command(text, marker):
        process.stdin.write(text + "\n")
        process.stdin.flush()
        while marker not in (line := process.stderr.readline()):
            assert line, process.wait(timeout=5)

    try:
        with Connection(**options) as client:
            client.create("stale", period="1d")
            command("cross stale @0;", "Complete rows=0")
            client.drop("stale")
            client.create("stale", period="1d")
            command("insert stale A @0 1 2 0 1 3 4 5;", "Committed rows=1")
            client.drop("stale")
            client.create("stale", period="1d")
            command("cross stale @0;", "Complete rows=0")
            client.drop("stale")
        process.stdin.close()
        assert process.wait(timeout=10) == 0
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
        process.stdout.close()
        process.stderr.close()


def check_terminal_session(shell_arguments, root):
    """An interactive session announces its connection; --output defaults to TSV."""
    output = root / "terminal-output"
    terminal = Terminal(shell_arguments[0], root, options=shell_arguments[1:])
    try:
        terminal.send(f'cross bars_5d "20260901" --output "{output}";\n'.encode())
        terminal.until(b"Connected to unix:")
        terminal.until(b"access=read-write")
        terminal.until(b"Complete rows=")
        terminal.until(b"ohlc> ")
        terminal.send(b'set preview 1; series bars_3m AAPL from "20260901 09:30:00" '
                      b'and "20260901 09:33:00";\n')
        terminal.until(b"Preview truncated: shown=1, total=2.")
        terminal.until(b"ohlc> ")
        terminal.close()
    finally:
        terminal.cleanup()
    assert output.read_text().splitlines()[0].count("\t") == 8


def check_cross_filter_frames(socket_path, token):
    """Reject malformed counted name lists without losing frame alignment."""
    def receive_exact(peer, size):
        data = b""
        while len(data) < size:
            part = peer.recv(size - len(data))
            assert part
            data += part
        return data

    with socket.socket(socket.AF_UNIX) as peer:
        peer.settimeout(5)
        peer.connect(socket_path)
        requests = [(1, struct.pack("<I", len(token)) + token, 0),
                    (5, struct.pack("<IIIQ", 1, 0, 0, 1 << 32), 1),
                    (5, struct.pack("<IIIQ", 1, 0, 1, 0), 1),
                    (5, struct.pack("<IIIQ", 1, 0, 0, 0), 0),
                    (6, struct.pack("<III", 1, 0, 65537), 3),
                    (6, struct.pack("<IIII", 1, 0, 1, 4097) + b"x", 1),
                    (6, struct.pack("<III", 1, 0, 0) + b"extra", 1),
                    (6, struct.pack("<IIII", 1, 0, 1, 0) + b"x", 1),
                    (6, struct.pack("<III", 1, 0, 1), 1),
                    (6, struct.pack("<III", 1, 0, 0), 0),
                    (2, b"", 0)]
        for request_id, (opcode, body, expected) in enumerate(requests, 1):
            peer.sendall(struct.pack("<4sHHIIQII", b"OHLC", 6, opcode, 0, len(body), request_id, 0, 0) + body)
            header = struct.unpack("<4sHHIIQII", receive_exact(peer, 32))
            assert header[2] == opcode and header[3] == 3 and header[5] == request_id
            assert header[6] == expected, header
            receive_exact(peer, header[4])


def main():
    server = os.path.abspath(sys.argv[1])
    library = os.path.abspath(sys.argv[2])
    with tempfile.TemporaryDirectory(prefix="ohlc-net-", dir=os.environ.get("TMPDIR", "/tmp")) as directory:
        root = Path(directory)
        socket_path = str(root / "socket")
        token = root / "write-token"
        token.write_bytes(b"test-writer\n")
        token.chmod(0o600)
        read_token = root / "read-token"
        read_token.write_bytes(b"test-reader\n")
        read_token.chmod(0o600)
        arguments = [server, "--data", str(root / "db"), "--socket", socket_path,
                     "--write-token-file", str(token), "--read-token-file", str(read_token),
                     "--memory-mib", "64", "--cache-mib", "4"]
        options = dict(socket=socket_path, token=b"test-writer", library=library)
        process = subprocess.Popen(arguments, stderr=subprocess.PIPE)
        try:
            with connect_when_ready(process, **options) as client:
                client.ping()
                uuid = client.uuid
                minute = client.create("bars_3m", period="3m", timezone="Asia/Shanghai")
                day = client.create("bars_5d", period="5d")
                require_error(11, lambda: client.create("bars_3m", timezone="UTC"))
                code, _ = client.register(1, "AAPL")
                binary, _ = client.register(1, b"00\0\xff\n")
                assert client.register(1, "AAPL")[0] == code
                assert list(client.dictionary(1)) == [(code, b"AAPL"), (binary, b"00\0\xff\n")]
                assert [table.name for table in client.tables()] == ["bars_3m", "bars_5d"]
                key = minute.time_key("20260901 09:30:00")
                assert minute.time_key("2026-09-01T01:30:00Z") == key
                row = (-(1 << 63), (1 << 63) - 1, -1, 0, (1 << 64) - 1, (1 << 64) - 1, 0, -(1 << 31))
                count = 17000
                encoded = bytearray()
                for index in range(count):
                    encoded.extend(struct.pack("<IIqqqqQQQiI", code, key + index * 3, *row, 255))
                sequence = minute.write_encoded(encoded)
                minute.insert(b"00\0\xff\n", key, (0,) * 7)
                day.insert("AAPL", "20260901", row)
                client.checkpoint()
                stats = client.stats()
                assert stats["commit_seq"] == stats["checkpoint_seq"]
                assert stats["table_count"] == 2 and stats["ticker_count"] == 3
                received = []
                with minute.series("AAPL", key, key + (count - 1) * 3) as query:
                    chunks = list(query)
                assert chunks[-1].final
                assert sum(chunk.count for chunk in chunks) == count
                assert len({chunk.snapshot_sequence for chunk in chunks}) == 1
                assert chunks[0].snapshot_sequence >= sequence
                for chunk in chunks:
                    received.extend(chunk.rows())
                assert received == [(key + i * 3, *row) for i in range(count)]
                with minute.cross(key) as query:
                    chunks = list(query)
                assert len(chunks) == 1 and chunks[0].final and chunks[0].count == 2
                rows = list(chunks[0].rows())
                assert rows == [(code, *row), (binary, *((0,) * 7), None)]
                with day.series("AAPL", "20260901", "20260901") as query:
                    assert sum(chunk.count for chunk in query) == 1
                require_error(1, lambda: minute.write_encoded(encoded[:72] * 2))
                with minute.cross(key - 1) as query:
                    assert sum(chunk.count for chunk in query) == 0
            if len(sys.argv) > 3:
                java = sys.argv[4] if len(sys.argv) > 4 else "java"
                subprocess.run([java, "-ea", "-cp", sys.argv[3], "JavaClientTest", socket_path],
                               check=True, timeout=30)
                with Connection(**options) as client:
                    with client.table("java_day").cross("20260901") as query:
                        result = [row for chunk in query for row in chunk.rows()]
                    assert result[0][1:] == (-(1 << 63), (1 << 63) - 1, 0, -1, (1 << 64) - 1,
                                              (1 << 64) - 1, (1 << 64) - 1, (1 << 31) - 1)
            shell = str(Path(server).with_name("ohlc"))
            shell_arguments = [shell, "--socket", socket_path, "--token-file", str(token)]
            def execute(command, expected=0):
                result = subprocess.run(shell_arguments + ["--execute", command],
                                        capture_output=True, text=True, timeout=30)
                assert result.returncode == expected, (result.returncode, result.stdout, result.stderr)
                return result
            with Connection(**options) as client:
                check_nullable(client.create("nullable", period="1d"))
                client.drop("nullable")
            check_shell_queries(execute, options)
            check_extended_periods(execute, options)
            check_cross_filter_frames(socket_path, b"test-writer")
            help_result = subprocess.run([shell, "--help"], capture_output=True, text=True, check=True)
            assert "help examples" in help_result.stdout
            dropped = execute('help drop; create discard --period 1d; drop discard; '
                              'create discard --period 1d; drop discard;')
            assert dropped.stderr.count("Dropped discard commit_seq=") == 2
            created_ids = [int(line.split("\t")[0]) for line in dropped.stdout.splitlines()
                           if "\tdiscard\t" in line]
            assert len(created_ids) == 2 and created_ids[1] > created_ids[0]
            with Connection(**options) as client:
                require_error(2, lambda: client.table("discard"))
                transient = client.create("python_drop", period="1d")
                client.drop("python_drop")
                assert all(table.name != "python_drop" for table in client.tables())
                with transient.cross("20260901") as query:
                    require_error(2, lambda: next(query))
            assert "commit_seq=" in execute("checkpoint; stats;").stdout
            execute("help nonexistent;", 1)
            # Command structure is checked before connecting to the server.
            missing = subprocess.run([shell, "--socket", str(root / "missing"),
                                      "--execute", "series t;"],
                                     capture_output=True, text=True, timeout=30)
            assert missing.returncode == 2, missing
            execute("frobnicate;", 2)
            # Zone names match exactly and must name a TZif file.
            for zone, message in (("asia/shanghai", "not found"), ("Asia", "not found"),
                                  ("+VERSION", "not found"), ("Asia//Shanghai", "invalid argument")):
                result = execute(f'create bad_zone --period 1m --timezone "{zone}";', 1)
                assert result.stderr == message + "\n", (zone, result.stderr)
            # A failure in nested scripts is reported once.
            inner = root / "inner.ohlc"
            inner.write_text("cross missing_table @0;\n")
            outer = root / "outer.ohlc"
            outer.write_text(f'source "{inner}";\n')
            assert execute(f'source "{outer}";', 1).stderr.count("not found") == 1
            check_stale_table(shell_arguments, options)
            check_terminal_session(shell_arguments, root)
            formatted = execute('cross bars_3m "20260901 09:30:00" --format table;')
            assert "2026-09-01T09:30:00+08:00" in formatted.stdout
            execute('cross bars_3m "unterminated', 2)
            execute('insert bars_5d SHELL "20260901" -2147483648 2147483647 0 -1 4294967295 18446744073709551615 4294967295;')
            execute("create shell_nullable --period 1d;")
            execute("insert shell_nullable N @0 -9223372036854775808 9223372036854775807 "
                    "NULL 0 18446744073709551615 null 18446744073709551615 -2147483648;")
            actual = json.loads(execute("cross shell_nullable @0 --format jsonl;").stdout)
            assert actual == dict(key=0, open="-9223372036854775808", high="9223372036854775807",
                                  low=None, close="0", volume="18446744073709551615",
                                  amount=None, adjust_factor="18446744073709551615",
                                  reserved="-2147483648")
            execute("insert shell_nullable N @0 NULL NULL NULL NULL NULL NULL NULL;")
            assert all(value is None for field, value in
                       json.loads(execute("cross shell_nullable @0 --format jsonl;").stdout).items()
                       if field != "key")
            assert execute("cross shell_nullable @1 --format jsonl;").stdout == ""
            for value in ("2147483648", "-2147483649"):
                execute(f"insert shell_nullable REJECTED @0 0 0 0 0 0 0 0 {value};", 1)
            for value in ("9223372036854775808", "-9223372036854775809"):
                execute(f"insert shell_nullable REJECTED @0 {value} 0 0 0 0 0 0;", 1)
            nullable_csv = root / "nullable.csv"
            nullable_csv.write_text("ticker,time,open,high,low,close,volume,amount,adjust_factor,reserved\n"
                                    "N,@2,,NULL,0,-1,18446744073709551615,0,null,0\n"
                                    "A\\B,@3,1,2,0,1,3,4,5,6\n")
            execute(f'import shell_nullable "{nullable_csv}" --format csv;')
            # CSV has no backslash escapes, matching CSV export.
            execute('resolve shell_nullable "A\\\\B";')
            # Without a header, the first row fixes the column count.
            mixed = root / "mixed.tsv"
            mixed.write_text("M\t@4\t1\t2\t0\t1\t3\t4\t5\nM\t@5\t1\t2\t0\t1\t3\t4\t5\t6\n")
            assert "confirmed_rows=0" in execute(
                f'import shell_nullable "{mixed}" --no-header;', 1).stderr
            actual = json.loads(execute("cross shell_nullable @2 --format jsonl;").stdout)
            assert actual["open"] is None and actual["high"] is None
            assert actual["reserved"] == "0" and actual["adjust_factor"] is None
            assert "NULL,NULL,0,-1,18446744073709551615,0,NULL,0" in execute(
                "cross shell_nullable @2 --format csv;").stdout
            execute("drop shell_nullable;")
            output = root / "result.jsonl"
            execute(f'cross bars_5d "20260901" --format jsonl --output "{output}";')
            assert '"amount":"18446744073709551615"' in output.read_text()
            mask = os.umask(0)
            os.umask(mask)
            assert output.stat().st_mode & 0o777 == 0o666 & ~mask
            original = output.read_bytes()
            existing = execute(f'cross bars_5d "20260901" --output "{output}";', 1)
            assert "already exists" in existing.stderr
            assert output.read_bytes() == original
            assert not list(root.glob("*.ohlc-*"))
            import_path = root / "partial.tsv"
            import_path.write_text("ticker\ttime\topen\thigh\tlow\tclose\tvolume\tamount\tadjust_factor\n"
                                   "IMPORT\t20260902\t1\t2\t3\t4\t5\t6\t7\n"
                                   "IMPORT\t20260903\t1\t2\t3\t4\t5\t18446744073709551616\t7\n")
            partial = execute(f'import bars_5d "{import_path}" --batch-rows 1;', 1)
            assert "confirmed_rows=1" in partial.stderr
            # Rows whose names would overflow one frame are committed as smaller batches.
            long_name = "L" * 4000
            long_path = root / "long-names.tsv"
            first_day = datetime.date(2000, 1, 1)
            with long_path.open("w") as stream:
                stream.write("ticker\ttime\topen\thigh\tlow\tclose\tvolume\tamount"
                             "\tadjust_factor\n")
                for offset in range(4200):
                    label = (first_day + datetime.timedelta(days=offset)).strftime("%Y%m%d")
                    stream.write(f"{long_name}\t{label}\t1\t2\t3\t4\t5\t6\t7\n")
            imported = execute(f'create long_names --period 1d; import long_names "{long_path}";')
            assert "Import complete: confirmed_batches=2 confirmed_rows=4200" in imported.stderr
            with Connection(**options) as client:
                with client.table("long_names").series(long_name, "20000101", "20991231") as query:
                    assert sum(chunk.count for chunk in query) == 4200
            execute('resolve bars_3m "NOT_REGISTERED"; register bars_3m "MUST_NOT_RUN";', 1)
            with Connection(**options) as client:
                require_error(2, lambda: client.resolve(1, "MUST_NOT_RUN"))
                with client.table("bars_5d").cross("20260902") as query:
                    result = [row for chunk in query for row in chunk.rows()]
                assert len(result) == 1 and result[0][1:] == (1, 2, 3, 4, 5, 6, 7, None)
            require_error(8, lambda: Connection(socket=socket_path, token=b"wrong", library=library))
            with Connection(socket=socket_path, token=b"test-reader", library=library) as reader:
                reader.ping()
                assert reader.stats()["table_count"] >= 2
                require_error(8, reader.checkpoint)
                require_error(8, lambda: reader.drop("bars_5d"))
                require_error(8, lambda: reader.register(1, "DENIED"))
                with reader.table("bars_3m").cross(key) as query:
                    assert sum(chunk.count for chunk in query) == 2
            # Oversized untrusted headers are rejected before body allocation.
            with socket.socket(socket.AF_UNIX) as peer:
                peer.settimeout(5)
                peer.connect(socket_path)
                peer.sendall(struct.pack("<4sHHIIQII", b"OHLC", 6, 1, 0, 0xffffffff, 1, 0, 0))
                assert peer.recv(1) == b""
            # Unknown protocol versions are rejected before processing requests.
            with socket.socket(socket.AF_UNIX) as peer:
                peer.settimeout(5)
                peer.connect(socket_path)
                peer.sendall(struct.pack("<4sHHIIQII", b"OHLC", 0xffff, 1, 0, 0, 1, 0, 0))
                assert peer.recv(1) == b""
        finally:
            process.send_signal(signal.SIGTERM)
            _, errors = process.communicate(timeout=20)
            assert process.returncode == 0, errors.decode()
        # Every acknowledged value remains available after a service restart, and
        # crash garbage after the last frame is discarded with a warning.
        with max((root / "db").glob("wal-*.log")).open("ab") as stream:
            stream.write(b"\x01" * 4096)
        process = subprocess.Popen(arguments, stderr=subprocess.PIPE)
        try:
            with connect_when_ready(process, **options) as client:
                assert client.uuid == uuid
                with client.table("bars_3m").series("AAPL", key, key + (count - 1) * 3) as query:
                    assert sum(chunk.count for chunk in query) == count
        finally:
            process.send_signal(signal.SIGTERM)
            _, errors = process.communicate(timeout=20)
            assert process.returncode == 0, errors.decode()
        assert b"WARNING: recovery discarded 4096 unacknowledged WAL bytes" in errors
    check_memory_pressure(server, library)
    check_immediate_busy(server, library)
    print("network framing, C/Python, two queries, authorization, pressure and restart: OK")


if __name__ == "__main__":
    main()
