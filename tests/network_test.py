# SPDX-License-Identifier: Apache-2.0
"""Exercise the actual server, C client binding, framing and durable mutations."""

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


def check_shell_queries(execute, options):
    """Check inclusive shell bounds and server-side ticker-set parsing."""
    assert " protocol=5 " in execute("ping; status;").stderr
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
        execute(start + ' to "20260901 09:33:00";', 1)
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
        for clause in ("and ticker in ('AAPL',)", "and ticker in ('AAPL' 'GOOGL')",
                       "and ticker in ('AAPL'", "and ticker in (,AAPL)",
                       "and ticker in ('')", "and ticker in ('AAPL'))"):
            execute(prefix + " " + clause + ";", 1)
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
            peer.sendall(struct.pack("<4sHHIIQII", b"OHLC", 5, opcode, 0, len(body), request_id, 0, 0) + body)
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
                row = (-2147483648, 2147483647, -1, 0, 4294967295, 18446744073709551615, 0)
                count = 17000
                encoded = bytearray()
                for index in range(count):
                    encoded.extend(struct.pack("<IIiiiiIQI", code, key + index * 3, *row))
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
                    rows = [result for chunk in query for result in chunk.rows()]
                assert rows == [(code, *row), (binary, *((0,) * 7))]
                with day.series("AAPL", "20260901", "20260901") as query:
                    assert sum(chunk.count for chunk in query) == 1
                require_error(1, lambda: minute.write_encoded(encoded[:40] * 2))
                with minute.cross(key - 1) as query:
                    assert sum(chunk.count for chunk in query) == 0
            if len(sys.argv) > 3:
                java = sys.argv[4] if len(sys.argv) > 4 else "java"
                subprocess.run([java, "-ea", "-cp", sys.argv[3], "JavaClientTest", socket_path],
                               check=True, timeout=30)
                with Connection(**options) as client:
                    with client.table("java_day").cross("20260901") as query:
                        result = [row for chunk in query for row in chunk.rows()]
                    assert result[0][1:] == (-2147483648, 2147483647, 0, -1, 4294967295,
                                              18446744073709551615, 4294967295)
            shell = str(Path(server).with_name("ohlc"))
            shell_arguments = [shell, "--socket", socket_path, "--token-file", str(token)]
            def execute(command, expected=0):
                result = subprocess.run(shell_arguments + ["--execute", command],
                                        capture_output=True, text=True, timeout=30)
                assert result.returncode == expected, (result.returncode, result.stdout, result.stderr)
                return result
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
            formatted = execute('cross bars_3m "20260901 09:30:00" --format table;')
            assert "2026-09-01T09:30:00+08:00" in formatted.stdout
            execute('cross bars_3m "unterminated', 2)
            execute('insert bars_5d SHELL "20260901" -2147483648 2147483647 0 -1 4294967295 18446744073709551615 4294967295;')
            output = root / "result.jsonl"
            execute(f'cross bars_5d "20260901" --format jsonl --output "{output}";')
            assert '"amount":"18446744073709551615"' in output.read_text()
            original = output.read_bytes()
            execute(f'cross bars_5d "20260901" --output "{output}";', 1)
            assert output.read_bytes() == original
            assert not list(root.glob("*.ohlc-*"))
            import_path = root / "partial.tsv"
            import_path.write_text("ticker\ttime\topen\thigh\tlow\tclose\tvolume\tamount\tadjust_factor\n"
                                   "IMPORT\t20260902\t1\t2\t3\t4\t5\t6\t7\n"
                                   "IMPORT\t20260903\t1\t2\t3\t4\t5\t18446744073709551616\t7\n")
            partial = execute(f'import bars_5d "{import_path}" --batch-rows 1;', 1)
            assert "confirmed_rows=1" in partial.stderr
            execute('resolve bars_3m "NOT_REGISTERED"; register bars_3m "MUST_NOT_RUN";', 1)
            with Connection(**options) as client:
                require_error(2, lambda: client.resolve(1, "MUST_NOT_RUN"))
                with client.table("bars_5d").cross("20260902") as query:
                    result = [row for chunk in query for row in chunk.rows()]
                assert len(result) == 1 and result[0][1:] == (1, 2, 3, 4, 5, 6, 7)
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
                peer.sendall(struct.pack("<4sHHIIQII", b"OHLC", 5, 1, 0, 0xffffffff, 1, 0, 0))
                assert peer.recv(1) == b""
            # Protocol 4 used an exclusive end and must not be silently accepted.
            with socket.socket(socket.AF_UNIX) as peer:
                peer.settimeout(5)
                peer.connect(socket_path)
                peer.sendall(struct.pack("<4sHHIIQII", b"OHLC", 4, 1, 0, 0, 1, 0, 0))
                assert peer.recv(1) == b""
        finally:
            process.send_signal(signal.SIGTERM)
            _, errors = process.communicate(timeout=20)
            assert process.returncode == 0, errors.decode()
        # Every acknowledged value remains available after a service restart.
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
    print("network framing, C/Python, two queries, authorization and restart: OK")


if __name__ == "__main__":
    main()
