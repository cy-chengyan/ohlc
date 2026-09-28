# SPDX-License-Identifier: Apache-2.0
"""Validate strict configuration, override order and startup without side effects."""

import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile

from network_test import connect_when_ready, require_error


def main():
    server = os.path.abspath(sys.argv[1])
    library = os.path.abspath(sys.argv[2])
    with tempfile.TemporaryDirectory(prefix="ohlc-config-", dir=os.environ.get("TMPDIR", "/tmp")) as temp:
        root = Path(temp)
        config = root / "ohlcd.conf"
        database = root / "database with spaces"
        socket = root / "socket"
        base = (f'# Settings\ndata = "{database}"\nsocket = {socket}\n'
                "memory-mib = 64\ncache-mib = 4\nmax-tables = 1\n"
                "wal-mib = 16\ndata-volume-mib = 8\n; End\n")
        config.write_text(base)

        def check(*args, status=0):
            result = subprocess.run([server, "--check-config", *args], capture_output=True,
                                    text=True, timeout=10)
            assert result.returncode == status, (args, result.stdout, result.stderr)
            assert not database.exists() and not socket.exists()
            return result

        check("--config", str(config))
        # Command-line overrides win on either side of --config.
        check("--memory-mib", "4", "--config", str(config), status=2)
        check("--config", str(config), "--memory-mib", "4", status=2)
        config.write_text(base.replace("memory-mib = 64", "memory-mib = 4"))
        check("--memory-mib", "64", "--config", str(config))
        check("--config", str(config), "--memory-mib", "64")
        check("--config", str(root / "missing"), status=2)
        check("--data", str(database), "--host", "0.0.0.0")
        check("--data", str(database), "--host", "not-an-address", status=2)
        check("--data", str(database), "--tls-cert", "missing", status=2)
        invalid = [b"unknown = 1", b"port = -1", b"port = 65536", b"port = 9tail",
                   b"cache-mib = 18446744073709551616", b"wal-mib = 15", b"read-workers = 17",
                   b"max-cursors = 0", b"host = \"unterminated", b"data = x\0host = y",
                   b" " * 8192, b"missing-equals", b"data = duplicate", b"cache-mib = 1 # comment"]
        for line in invalid:
            config.write_bytes(f"data = {database}\n".encode() + line + b"\n")
            result = check("--config", str(config), status=2)
            assert f"{config}:2:" in result.stderr, result.stderr
        config.write_text(base + "host = 127.0.0.1\n")
        check("--config", str(config), status=2)
        config.write_text(base + "cache-mib = 1\n")
        check("--config", str(config), status=2)
        token = root / "token"
        token.write_text("private-token\n")
        token.chmod(0o644)
        config.write_text(base + f"write-token-file = {token}\n")
        check("--config", str(config), status=2)
        token.chmod(0o600)
        check("--config", str(config))
        for host in ("0.0.0.0", "::"):
            remote = ["--data", str(database), "--host", host]
            check(*remote)
            check(*remote, "--write-token-file", str(token))
            config.write_text(f"data = {database}\nhost = {host}\nread-token-file = {token}\n")
            check("--config", str(config))
        config.write_text(base + f"write-token-file = {token}\n")
        arguments = [server, "--max-tables", "2", "--config", str(config)]
        process = subprocess.Popen(arguments, stderr=subprocess.PIPE)
        try:
            with connect_when_ready(process, socket=str(socket), token=b"private-token",
                                    library=library) as client:
                uuid = client.uuid
                table = client.create("daily", period="1d")
                client.create("five_day", period="5d")
                require_error(3, lambda: client.create("over_limit", period="1d"))
                client.register(1, "AAPL")
                table.insert("AAPL", "20260901", (1, 2, 3, 4, 5, 6, 7))
        finally:
            process.send_signal(signal.SIGTERM)
            _, errors = process.communicate(timeout=20)
            assert process.returncode == 0, errors.decode()
        process = subprocess.Popen(arguments, stderr=subprocess.PIPE)
        try:
            with connect_when_ready(process, socket=str(socket), token=b"private-token",
                                    library=library) as client:
                assert client.uuid == uuid
                with client.table("daily").cross("20260901") as query:
                    rows = [row for chunk in query for row in chunk.rows()]
                assert rows == [(0, 1, 2, 3, 4, 5, 6, 7)]
        finally:
            process.send_signal(signal.SIGTERM)
            _, errors = process.communicate(timeout=20)
            assert process.returncode == 0, errors.decode()
    print("configuration parsing, CLI precedence, check-only, startup and restart: OK")


if __name__ == "__main__":
    main()
