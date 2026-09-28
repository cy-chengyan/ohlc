# SPDX-License-Identifier: Apache-2.0
"""Exercise the actual server, C client binding, framing and durable mutations."""

import os
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
                with minute.series("AAPL", key, key + count * 3) as query:
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
                with day.series("AAPL", "20260901", "20260902") as query:
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
                peer.sendall(struct.pack("<4sHHIIQII", b"OHLC", 4, 1, 0, 0xffffffff, 1, 0, 0))
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
                with client.table("bars_3m").series("AAPL", key, key + count * 3) as query:
                    assert sum(chunk.count for chunk in query) == count
        finally:
            process.send_signal(signal.SIGTERM)
            _, errors = process.communicate(timeout=20)
            assert process.returncode == 0, errors.decode()
    print("network framing, C/Python, two queries, authorization and restart: OK")


if __name__ == "__main__":
    main()
