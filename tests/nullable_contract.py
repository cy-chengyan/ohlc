# SPDX-License-Identifier: Apache-2.0
"""Shared nullable-row checks for the embedded and network interfaces."""

import struct

from ohlc import Error


VALUES = (-(1 << 63), (1 << 63) - 1, -1, 0,
          (1 << 64) - 1, (1 << 64) - 1, (1 << 64) - 1, -(1 << 31))


def rows(query):
    with query:
        return [row for chunk in query for row in chunk.rows()]


def expected(mask):
    return tuple(value if mask & (1 << field) else None
                 for field, value in enumerate(VALUES))


def verify_nullable(table):
    for time in range(8):
        actual = rows(table.cross(time))
        assert actual == [(ticker, *expected(time * 32 + ticker)) for ticker in range(32)]
    assert rows(table.cross(1000)) == []
    assert rows(table.series("N0", 0, 0)) == [(0, *((None,) * 8))]


def check_nullable(table):
    table.write([(f"N{mask % 32}", mask // 32, expected(mask)) for mask in range(256)])
    verify_nullable(table)
    for index, value in enumerate((None, 0, (1 << 31) - 1, -(1 << 31))):
        values = (0,) * 7 if value is None else (0,) * 7 + (value,)
        table.insert("N0", 128 + index, values)
        assert rows(table.cross(128 + index)) == [(0, *((0,) * 7), value)]
    table.insert("N0", 128, (1,) * 7 + (123,))
    table.insert("N0", 128, (0,) * 7)
    assert rows(table.cross(128)) == [(0, *((0,) * 7), None)]
    # Every present field keeps its full declared range. Invalid batches are atomic.
    for field, bad in ((0, -(1 << 63) - 1), (0, 1 << 63), (4, -1), (4, 1 << 64),
                       (7, -(1 << 31) - 1), (7, 1 << 31), (1, True), (2, 1.5)):
        values = list(VALUES)
        values[field] = bad
        try:
            table.insert("INVALID", 0, values)
        except ValueError:
            pass
        else:
            raise AssertionError("Out-of-range or noninteger value accepted")
    try:
        table.resolve("INVALID")
    except Error as error:
        assert error.code == 2
    else:
        raise AssertionError("Rejected write created a ticker")
    for flags, payload in ((256, 0), (0, 1)):
        record = struct.pack("<IIqqqqQQQiI", 0, 1000, payload, 0, 0, 0, 0, 0, 0, 0, flags)
        try:
            table.write_encoded(record)
        except Error as error:
            assert error.code == 1
        else:
            raise AssertionError("Noncanonical encoded row accepted")
    assert rows(table.cross(1000)) == []
