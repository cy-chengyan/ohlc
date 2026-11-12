# SPDX-License-Identifier: Apache-2.0
"""Check the same closed-range contract through both Python transports."""

from ohlc import Error


def check_closed_series(table, key, stamp, values):
    table.insert("AAPL", key + 2, values)
    table.insert("AAPL", 0xffffffff, values)
    cases = (
        (stamp, stamp, [key]),
        (stamp, f"@{key + 1}", [key, key + 1]),
        (key + 1, key + 1, [key + 1]),
        (key + 3, key + 4, []),
        (0xffffffff, 0xffffffff, [0xffffffff]),
    )
    for start, end, expected in cases:
        with table.series("AAPL", start, end) as query:
            assert [row[0] for chunk in query for row in chunk.rows()] == expected
    for start, end in ((key + 1, key), (key, "@4294967296"), (key, 1 << 32), (key, -1)):
        try:
            with table.series("AAPL", start, end) as query:
                list(query)
        except Error as error:
            assert error.code == 1
        except ValueError:
            assert isinstance(end, int) and (end < 0 or end > 0xffffffff)
        else:
            raise AssertionError(f"Invalid closed range accepted: {start}, {end}")
