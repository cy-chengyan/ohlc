# SPDX-License-Identifier: Apache-2.0
"""Run with an unused database directory and OHLC_LIBRARY pointing to libohlc."""

import sys

from ohlc import Database, DatabaseOptions


def main():
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python embedded.py DATABASE_DIRECTORY")
    options = DatabaseOptions(memory_limit=256 << 20, cache_bytes=16 << 20)
    with Database(sys.argv[1], create=True, options=options) as db:
        table = db.create("bars_3m", period="3m", timezone="Asia/Shanghai")
        table.write([
            ("AAPL", "20260901 09:30:00", (10000, 10100, 9950, 10080, 1200, 12100000, 1000000)),
            ("AAPL", "20260901 09:33:00", (10080, 10120, 10000, 10100, 900, 9100000, 1000000)),
        ])
        with table.series("AAPL", "20260901 09:30:00", "20260901 09:36:00") as query:
            for chunk in query:
                for time_key, *row in chunk.rows():
                    print(table.format_time(time_key), row)
        with table.cross("20260901 09:30:00") as query:
            for chunk in query:
                print("Cross:", list(chunk.rows()))
        # Embedded applications schedule checkpoints; close does not do so.
        db.checkpoint()


if __name__ == "__main__":
    main()
