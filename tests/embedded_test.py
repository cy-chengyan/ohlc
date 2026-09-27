# SPDX-License-Identifier: Apache-2.0
"""Check embedded ownership, snapshots, integer widths and cross-language files."""

from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import struct
import shutil
import subprocess
import sys
import tempfile
import threading
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "clients" / "python"))
from ohlc import Database, DatabaseOptions, Error


EXTREMES = (-2147483648, 2147483647, -1, 0, 4294967295, 18446744073709551615, 4294967295)
BINARY = b"00\0\xff\n"


def require_error(code, operation):
    try:
        operation()
    except Error as error:
        assert error.code == code, error
    else:
        raise AssertionError(f"Expected status {code}")


def results(query):
    with query:
        return [row for chunk in query for row in chunk.rows()]


def concurrent_close(db, table, key):
    """Race independent reads and writes with close; all calls must finish safely."""
    barrier = threading.Barrier(4)

    def reader():
        barrier.wait(timeout=5)
        for _ in range(32):
            try:
                rows = results(table.cross(key))
                if rows:
                    assert rows[0][1:] == EXTREMES
            except Error as error:
                assert error.code == 5, error
                return

    def writer():
        barrier.wait(timeout=5)
        for i in range(16):
            try:
                table.insert("AAPL", key + 10000 + i, EXTREMES)
            except Error as error:
                assert error.code == 5, error
                return

    with ThreadPoolExecutor(max_workers=3) as workers:
        futures = [workers.submit(reader), workers.submit(reader), workers.submit(writer)]
        barrier.wait(timeout=5)
        db.close()
        for future in futures:
            future.result(timeout=10)


def python_contract(path, library):
    options = DatabaseOptions(memory_limit=64 << 20, cache_bytes=4 << 20, wal_segment_bytes=16 << 20)
    with Database(path, create=True, options=options, chunk_rows=17, library=library) as db:
        uuid = db.uuid
        require_error(4, lambda: Database(path, library=library))
        minute = db.create("bars_3m", period="3m", timezone="Asia/Shanghai", description="UTF-8 \U0001f4c8")
        day = db.create("bars_5d", period="5d")
        code, _ = db.register("AAPL")
        binary, _ = db.register(BINARY)
        assert list(db.dictionary()) == [(code, b"AAPL"), (binary, BINARY)]
        assert [item.name for item in db.tables()] == ["bars_3m", "bars_5d"]
        require_error(11, lambda: db.create("bars_3m", timezone="UTC"))
        require_error(2, lambda: db.resolve("MISSING"))
        key = minute.time_key("20260901 09:30:00")
        assert key == minute.time_key("2026-09-01T01:30:00Z")
        assert minute.time_key(minute.format_time(0xffffffff)) == 0xffffffff
        assert day.time_key(day.format_time(0xffffffff)) == 0xffffffff
        require_error(1, lambda: minute.time_key("20260901 09:30:01"))
        # Backfill earlier timestamps after the later data already exists.
        encoded = b"".join(struct.pack("<IIiiiiIQI", code, key + i * 3, *EXTREMES)
                           for i in range(600))
        minute.write_encoded(encoded[300 * 40:])
        minute.write_encoded(bytearray(encoded[:300 * 40]))
        require_error(1, lambda: minute.write_encoded(encoded[:40] * 2))
        minute.insert(BINARY, key, (0,) * 7)
        day.insert("AAPL", "20260901", EXTREMES)
        snapshot = minute.cross(key)
        minute.insert("AAPL", key, (1,) * 7)
        assert results(snapshot) == [(code, *EXTREMES), (binary, *((0,) * 7))]
        assert results(minute.cross(key))[0] == (code, *((1,) * 7))
        minute.insert("AAPL", key, EXTREMES)
        with minute.series("AAPL", key, key + 1800) as query:
            chunks = list(query)
        expected = [(key + i * 3, *EXTREMES) for i in range(600)]
        assert [row for chunk in chunks for row in chunk.rows()] == expected
        assert chunks[-1].final and len(chunks) > 2
        assert len({chunk.snapshot_sequence for chunk in chunks}) == 1
        # A buffer view with an offset and trailing bytes must retain its bounds.
        output = bytearray(b"x" * (19 * 36 + 7))
        received = []
        with minute.series("AAPL", key, key + 1800) as query:
            while True:
                count = query.read_into(memoryview(output)[3:-4])
                if not count:
                    break
                received.extend(struct.iter_unpack("<IiiiiIQI", output[3:3 + count * 36]))
        assert received == expected
        assert output[:3] == b"xxx" and output[-4:] == b"xxxx"
        with minute.cross(key) as query:
            retained = next(query)
        assert len(results(minute.cross(key))) == 2
        assert list(retained.rows()) == [(code, *EXTREMES), (binary, *((0,) * 7))]
        assert results(day.cross("20260901")) == [(code, *EXTREMES)]
        assert results(minute.cross(key - 1)) == []
        # A partially constructed query must release its native cursor once.
        with patch.object(db._lib, "ohlc_cursor_sequence", side_effect=MemoryError("allocation")):
            try:
                minute.cross(key)
            except MemoryError:
                pass
            else:
                raise AssertionError("Injected construction failure did not occur")
        assert len(results(minute.cross(key))) == 2
        db.checkpoint()
        assert db.stats()["checkpoint_seq"] == db.stats()["commit_seq"]
        # Leave a committed row in WAL and an unfinished cursor at close.
        day.insert(BINARY, "20260906", EXTREMES)
        outstanding = minute.cross(key)
    assert list(outstanding) == []
    assert list(retained.rows())[0] == (code, *EXTREMES)
    require_error(5, lambda: minute.cross(key))
    with Database(path, library=library) as db:
        assert db.uuid == uuid
        assert results(db.table("bars_5d").cross("20260906")) == [(binary, *EXTREMES)]
        concurrent_close(db, db.table("bars_3m"), key)
        db.close()
    return uuid


def table_deletion(path, library):
    options = DatabaseOptions(memory_limit=64 << 20, cache_bytes=4 << 20, max_tables=2)
    with Database(path, create=True, options=options, library=library) as db:
        keep = db.create("keep", period="1d")
        old = db.create("replace", period="1d")
        code, _ = db.register("AAPL")
        keep.insert("AAPL", "20260901", EXTREMES)
        old.insert("AAPL", "20260901", EXTREMES)
        db.checkpoint()
        db.checkpoint()
        old_files = path / "tables" / f"{old.id:08x}"
        assert old_files.is_dir()
        snapshot = old.cross("20260901")
        dropped = db.drop("replace")
        assert db.stats()["commit_seq"] == dropped
        assert db.stats()["table_count"] == 1
        require_error(2, lambda: db.table("replace"))
        require_error(2, lambda: db.drop("replace"))
        require_error(2, lambda: old.cross("20260901"))
        require_error(2, lambda: old.insert("AAPL", "20260901", EXTREMES))
        new = db.create("replace", period="1d")
        assert new.id > old.id
        new.insert("AAPL", "20260901", (1,) * 7)
        db.checkpoint()
        db.checkpoint()
        assert old_files.is_dir(), "A live snapshot must keep its table files"
        assert results(snapshot) == [(code, *EXTREMES)]
        db.checkpoint()
        assert not old_files.exists()
        assert results(new.cross("20260901")) == [(code, *((1,) * 7))]
        assert [item.id for item in db.tables()] == [keep.id, new.id]
        deleted_id = new.id

    # Only WAL records cover this deletion: closing the embedded handle does
    # not checkpoint. Recovery must not reopen the removed table or reuse its ID.
    with Database(path, options=options, library=library) as db:
        assert db.table("replace").id == deleted_id
        db.drop("replace")
    with Database(path, options=options, library=library) as db:
        require_error(2, lambda: db.table("replace"))
        previous = deleted_id
        # Cross the 256-slot directory boundary while keeping max_tables=2.
        for _ in range(260):
            table = db.create("replace", period="1d")
            assert table.id > previous
            previous = table.id
            db.drop("replace")
        last = db.create("replace", period="1d")
        last.insert("AAPL", "20260901", (2,) * 7)
        db.checkpoint()
        db.checkpoint()
        assert not (path / "tables" / f"{deleted_id:08x}").exists()
        assert len(list((path / "tables").iterdir())) == 2
        assert db.stats()["table_count"] == 2

    admin = str(Path(library).with_name("ohlc-admin"))
    archive = path.with_name("drop-backup.ohlc")
    restored = path.with_name("drop-restored")
    for arguments in (("check", path), ("backup", path, archive),
                      ("restore", archive, restored), ("check", restored)):
        result = subprocess.run([admin, *map(str, arguments)], check=True,
                                capture_output=True, text=True, timeout=15)
        if arguments[0] == "check":
            assert "verified_rows=2 tables=2" in result.stdout

    # Losing the newer CURRENT entry must not resurrect a table whose files
    # have already been reclaimed; the other checkpoint is independently valid.
    fallback = path.with_name("drop-fallback")
    shutil.copytree(path, fallback)
    slots = [fallback / "CURRENT.0", fallback / "CURRENT.1"]
    newest = max(slots, key=lambda slot: struct.unpack_from("<Q", slot.read_bytes(), 32)[0])
    damaged = bytearray(newest.read_bytes())
    damaged[0] ^= 1
    newest.write_bytes(damaged)
    with Database(fallback, options=options, library=library) as db:
        assert [item.id for item in db.tables()] == [keep.id, last.id]
        assert results(db.table("keep").cross("20260901")) == [(code, *EXTREMES)]
        assert results(db.table("replace").cross("20260901")) == [(code, *((2,) * 7))]


def main():
    library = os.path.abspath(sys.argv[1])
    with tempfile.TemporaryDirectory(prefix="ohlc-embed-", dir=os.environ.get("TMPDIR", "/tmp")) as temp:
        path = Path(temp) / "db"
        table_deletion(Path(temp) / "drop", library)
        uuid = python_contract(path, library)
        if len(sys.argv) > 2:
            jni, classes, java = sys.argv[2:5]
            subprocess.run([java, "-Xcheck:jni", "-ea", f"-Dohlc.jni.library={os.path.abspath(jni)}",
                            "-cp", classes, "JavaEmbeddedTest", str(path)], check=True, timeout=45)
            with Database(path, library=library) as db:
                assert db.uuid == uuid
                table = db.table("java_day")
                assert table.description == "JNI \U0001f680"
                rows = results(table.cross("20260901"))
                assert rows == [(db.resolve("JAVA"), *EXTREMES)]
                assert len(results(table.series("JAVA", "20260901", "20260905"))) == 4
    print("embedded Python, snapshots, buffer ownership, close races and file recovery: OK")


if __name__ == "__main__":
    main()
