# SPDX-License-Identifier: Apache-2.0
"""Verify offline ownership, complete backups and rejection of damaged archives."""

import hashlib
import os
from pathlib import Path
import struct
import subprocess
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "clients" / "python"))
from ohlc import Database


def main():
    tool = str(Path(sys.argv[1]).resolve())
    library = str(Path(sys.argv[2]).resolve())

    def run(*arguments, success=True):
        result = subprocess.run([tool, *map(str, arguments)], capture_output=True,
                                text=True, timeout=60)
        assert (result.returncode == 0) == success, (arguments, result.stdout, result.stderr)
        return result.stdout

    with tempfile.TemporaryDirectory(prefix="ohlc-admin-", dir=os.environ.get("TMPDIR", "/tmp")) as temporary:
        root = Path(temporary)
        source = root / "source"
        archive = root / "backup.ohlc"
        restored = root / "restored"
        row = (-2147483648, 2147483647, -1, 0, 4294967295, 18446744073709551615, 4294967295)
        with Database(source, create=True, library=library) as db:
            db.register("AAPL")
            minute = db.create("minute", timezone="UTC")
            day = db.create("day", period="5d")
            minute.insert("AAPL", "20260901 09:30:00", row)
            minute.insert("AAPL", "20260901 09:29:00", row)
            day.insert("AAPL", "20260901", row)
            uuid = db.uuid
            sequence = db.stats()["commit_seq"]
            run("backup", source, archive, success=False)
            assert not archive.exists()
            run("check", source, success=False)
        assert "verified_rows=3" in run("check", source)
        run("backup", source, archive)
        original = archive.read_bytes()
        assert archive.stat().st_mode & 0o777 == 0o600
        run("backup", source, archive, success=False)
        assert archive.read_bytes() == original
        run("restore", archive, restored)
        assert "verified_rows=3" in run("check", restored)
        with Database(restored, library=library) as db:
            assert db.uuid == uuid and db.stats()["commit_seq"] == sequence
            with db.table("minute").series("AAPL", "20260901 09:29:00", "20260901 09:31:00") as query:
                rows = [value for chunk in query for value in chunk.rows()]
            assert len(rows) == 2 and rows[0][0] < rows[1][0]
            assert all(value[1:] == row for value in rows)
        marker = restored / "do-not-overwrite"
        marker.write_text("preserve")
        run("restore", archive, restored, success=False)
        assert marker.read_text() == "preserve"
        invalid = root / "invalid.ohlc"
        for contents in (original[:-1], original[:-32] + bytes(32), original + b"extra"):
            invalid.write_bytes(contents)
            run("restore", invalid, root / "invalid-db", success=False)
            assert not (root / "invalid-db").exists()
        path = b"../escaped"
        contents = original[:36] + struct.pack("<IQ", len(path), 1) + path + b"X" + bytes(12)
        invalid.write_bytes(contents + hashlib.sha256(contents).digest())
        run("restore", invalid, root / "invalid-db", success=False)
        assert not (root / "escaped").exists()
        fifo = root / "fifo"
        os.mkfifo(fifo)
        run("restore", fifo, root / "fifo-db", success=False)
        assert not (root / "fifo-db").exists()
        (source / "linked-file").symlink_to(root / "outside")
        run("backup", source, root / "symlink.ohlc", success=False)
        assert not (root / "symlink.ohlc").exists()
        data = next((restored / "tables").rglob("data-*.dat"))
        with data.open("r+b") as file:
            file.seek(-1, os.SEEK_END)
            byte = file.read(1)[0]
            file.seek(-1, os.SEEK_END)
            file.write(bytes([byte ^ 0xff]))
        run("check", restored, success=False)
        assert not list(root.glob("*.partial.*"))
    print("Offline backup, restore, ownership, checksums and path boundaries passed")


if __name__ == "__main__":
    main()
