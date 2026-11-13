#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Measure extent growth under deterministic append and historical replacement."""

import argparse
import hashlib
import json
from pathlib import Path
import struct
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "clients" / "python"))
from ohlc import Database, DatabaseOptions


WRITE = struct.Struct("<IIqqqqQQQiI")


def values(ticker, index, version):
    fields = [index + 1, (1 << 63) - 1 - ticker, -index - 7, version, ticker,
              (version << 32) | index, (1 << 64) - 1 - index, -ticker - 1]
    fields[index % 3] = None
    return tuple(fields)


def write_range(table, tickers, first_key, start, count, version):
    buffer = bytearray(tickers * count * WRITE.size)
    offset = 0
    for index in range(start, start + count):
        flags = 255 ^ (1 << (index % 3))
        for ticker in range(tickers):
            fields = values(ticker, index, version)
            WRITE.pack_into(buffer, offset, ticker, first_key + index * 60,
                            *(0 if field is None else field for field in fields), flags)
            offset += WRITE.size
    table.write_encoded(buffer)


def emit(record):
    print(json.dumps(record, sort_keys=True), flush=True)


def measure(path, tickers, times, round_number, start):
    data = sorted(path.glob("tables/*/data-*.dat"))
    metadata = sorted(path.glob("tables/*/data-*.meta"))
    sizes = [entry.stat().st_size for entry in data]
    physical = sum((size - 4096) // 8192 for size in sizes)
    live = (tickers // 16) * ((times + 7) // 8)
    emit({"event": "space", "round": round_number, "rows": tickers * times,
          "P": physical, "Q": live, "P_over_Q": physical / live,
          "data_bytes": sum(sizes), "metadata_bytes": sum(p.stat().st_size for p in metadata),
          "volumes": len(data), "seconds": time.monotonic() - start})


def verify_cross(table, tickers, first_key, index, version):
    seen = 0
    with table.cross(first_key + index * 60) as query:
        for chunk in query:
            for row in chunk.rows():
                expected = (seen, *values(seen, index, version))
                if row != expected:
                    raise AssertionError((row, expected))
                seen += 1
    if seen != tickers:
        raise AssertionError((seen, tickers))


def verify_all(table, tickers, first_key, versions):
    seen = 0
    for ticker in range(tickers):
        index = 0
        with table.series(f"S{ticker}", first_key, first_key + (len(versions) - 1) * 60) as query:
            for chunk in query:
                for row in chunk.rows():
                    expected = (first_key + index * 60, *values(ticker, index, versions[index]))
                    if row != expected:
                        raise AssertionError((ticker, index, row, expected))
                    index += 1
        if index != len(versions):
            raise AssertionError((ticker, index, len(versions)))
        seen += index
    return seen


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--library", type=Path, required=True)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--tickers", type=int, default=256)
    parser.add_argument("--seed-times", type=int, default=2048)
    parser.add_argument("--rounds", type=int, default=256)
    args = parser.parse_args()
    if args.database.exists():
        parser.error("--database must not exist; measurements never overwrite a database")
    if args.tickers <= 0 or args.tickers % 16 or args.seed_times < 128 or args.seed_times % 128:
        parser.error("tickers must be a positive multiple of 16; seed-times a multiple of 128")
    if args.rounds <= 0 or args.tickers * 128 > 200000:
        parser.error("rounds must be positive and seed batches must fit max_write_rows")
    library = args.library.resolve()
    emit({"event": "environment", "library": str(library),
          "library_sha256": hashlib.sha256(library.read_bytes()).hexdigest(),
          "database": str(args.database), "tickers": args.tickers,
          "seed_times": args.seed_times, "rounds": args.rounds,
          "append_times_per_round": 8, "replacement_times_per_round": 8,
          "workload": "unpaced synthetic minute bars, checkpoint after every pair of batches"})
    options = DatabaseOptions(memory_limit=512 << 20, cache_bytes=8 << 20,
                              data_volume_bytes=4096 + 8192 * 8192,
                              wal_segment_bytes=16 << 20)
    versions = [1] * args.seed_times
    start = time.monotonic()
    with Database(args.database, create=True, options=options, library=library) as db:
        table = db.create("bars", period="1m", timezone="UTC")
        first_key = table.time_key("2026-01-01T00:00:00Z")
        for ticker in range(args.tickers):
            code, _ = db.register(table.id, f"S{ticker}")
            if code != ticker:
                raise AssertionError((code, ticker))
        for first in range(0, args.seed_times, 128):
            write_range(table, args.tickers, first_key, first, 128, 1)
            db.checkpoint()
        measure(args.database, args.tickers, len(versions), 0, start)
        for iteration in range(1, args.rounds + 1):
            version = iteration + 1
            previous_times = len(versions)
            write_range(table, args.tickers, first_key, previous_times, 8, version)
            versions.extend([version] * 8)
            historical = ((iteration * 37) % (previous_times // 8)) * 8
            write_range(table, args.tickers, first_key, historical, 8, version)
            versions[historical:historical + 8] = [version] * 8
            db.checkpoint()
            verify_cross(table, args.tickers, first_key, historical, version)
            verify_cross(table, args.tickers, first_key, previous_times, version)
            if iteration % 32 == 0 or iteration == args.rounds:
                measure(args.database, args.tickers, len(versions), iteration, start)
        checked = verify_all(table, args.tickers, first_key, versions)
        emit({"event": "verified", "phase": "before_close", "rows": checked,
              "seconds": time.monotonic() - start, "stats": db.stats()})
    with Database(args.database, options=options, library=library) as db:
        checked = verify_all(db.table("bars"), args.tickers, first_key, versions)
    emit({"event": "verified", "phase": "after_reopen", "rows": checked,
          "seconds": time.monotonic() - start})


if __name__ == "__main__":
    main()
