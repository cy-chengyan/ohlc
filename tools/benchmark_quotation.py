#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Read immutable quotation samples and derive the dense C comparison workload."""

import csv
import datetime as dt
from decimal import Decimal, ROUND_HALF_EVEN
import hashlib
import io
import json
import os
from pathlib import Path
import struct
import sys

import numpy as np

from benchmark_metrics import atomic_json, sha256

VALUE = np.dtype([(name, kind) for name, kind in zip(
    ("open", "high", "low", "close", "volume", "amount", "adjust_factor", "reserved", "flags"),
    ("<i8", "<i8", "<i8", "<i8", "<u8", "<u8", "<u8", "<i4", "<u4"))])
WRITE = np.dtype([("ticker", "<u4"), ("time", "<u4"), ("value", VALUE)])
RESULT = np.dtype([("key", "<u4"), ("value", VALUE)])
MINUTES = [f"{m // 60:02}:{m % 60:02}:00" for m in
           list(range(9 * 60 + 31, 11 * 60 + 31)) + list(range(13 * 60 + 1, 15 * 60 + 1))]


def load_source(path, manifest):
    """Open only for reading, and fingerprint the exact bytes that were parsed."""
    before = path.stat()
    with path.open("rb") as stream:
        data = stream.read()
    after = path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise RuntimeError(f"Source changed while reading: {path}")
    manifest[str(path)] = {"sha256": hashlib.sha256(data).hexdigest(),
                           "size": after.st_size, "mtime_ns": after.st_mtime_ns}
    return list(csv.reader(io.StringIO(data.decode("utf-8-sig"))))


def prepare(source, destination):
    source = source.resolve(strict=True)
    destination = destination.resolve()
    if destination == source or source in destination.parents:
        raise ValueError("Output must be outside the immutable quotation tree")
    destination.mkdir(parents=True, exist_ok=False)
    months = [source / name for name in ("20220301", "20220401", "20220501", "20220601")]
    manifest = {}
    baseline = {}
    for month in months:
        for row in load_source(month / "000001.SZ", manifest):
            timestamp = row[2]
            if timestamp[11:] in MINUTES:
                if timestamp in baseline:
                    raise ValueError(f"Duplicate baseline key: {timestamp}")
                baseline[timestamp] = row
    dates = sorted({key[:10] for key in baseline})[:64]
    if len(dates) != 64:
        raise ValueError("Need 64 observed source dates")
    timestamps = [date + " " + minute for date in dates for minute in MINUTES]
    if any(key not in baseline for key in timestamps):
        raise ValueError("Baseline security must cover every selected minute")
    positions = {key: index for index, key in enumerate(timestamps)}
    tickers = sorted(path.name for path in months[0].iterdir()
                     if path.is_file() and path.suffix in (".SZ", ".SH", ".BJ"))[:256]
    if len(tickers) != 256:
        raise ValueError("Need 256 seed securities")
    seed = np.lib.format.open_memmap(destination / "seed.npy", mode="w+", dtype=VALUE,
                                   shape=(len(tickers), len(timestamps)))
    rounding = {name: 0 for name in VALUE.names[:6]}
    fallback = 0

    def convert(row):
        values = []
        for name, index, scale in zip(VALUE.names[:6], (3, 5, 6, 4, 7, 8),
                                      (10000, 10000, 10000, 10000, 1, 100)):
            number = Decimal(row[index]) * scale
            rounded = number.to_integral_value(rounding=ROUND_HALF_EVEN)
            rounding[name] += int(number != rounded)
            value = int(rounded)
            lower, upper = (-(1 << 63), 1 << 63) if index in (3, 4, 5, 6) else (0, 1 << 64)
            if not lower <= value < upper:
                raise ValueError(f"Out of range {name}: {row}")
            values.append(value)
        return (*values, 1000000, 0, 127)

    for stock, ticker in enumerate(tickers):
        observed = {}
        for month in months:
            path = month / ticker
            if not path.is_file():
                continue
            for row in load_source(path, manifest):
                timestamp = row[2]
                if timestamp not in positions:
                    continue
                if row[1] != ticker or row[9] != timestamp[:10].replace("-", ""):
                    raise ValueError(f"Source ticker/date mismatch: {path}")
                if timestamp in observed:
                    raise ValueError(f"Duplicate source key: {path}: {timestamp}")
                if all(row[index] for index in (3, 4, 5, 6, 7, 8)):
                    observed[timestamp] = row
        for timestamp, position in positions.items():
            row = observed.get(timestamp)
            if row is None:
                fallback += 1
                row = baseline[timestamp]
            seed[stock, position] = convert(row)
        if stock % 32 == 0:
            print(f"seed securities: {stock + 1}/{len(tickers)}", flush=True)
    seed.flush()
    zone = dt.timezone(dt.timedelta(hours=8))
    keys = [int(dt.datetime.fromisoformat(value).replace(tzinfo=zone).timestamp()) // 60
            for value in timestamps]
    # Extra timestamps are explicit generator dates, not engine calendar inference.
    day = dt.date.fromisoformat(dates[-1])
    while len(keys) < len(timestamps) + 301:
        day += dt.timedelta(days=1)
        if day.weekday() < 5:
            keys.extend(int(dt.datetime.fromisoformat(str(day) + " " + minute)
                            .replace(tzinfo=zone).timestamp()) // 60 for minute in MINUTES)
    atomic_json(destination / "manifest.json", {
        "source": str(source), "sources": manifest, "seed_tickers": tickers,
        "dates": dates, "keys": keys, "seed_shape": list(seed.shape),
        "seed_sha256": sha256(destination / "seed.npy"), "fallback_rows": fallback,
        "rounding_counts": rounding, "scale": {"prices": 10000, "volume": 1, "amount": 100},
        "rounding": "decimal ROUND_HALF_EVEN", "adjust_factor": 1000000,
        "reserved": None, "flags": 127, "source_omissions": ["idx", "pre_close", "09:30"],
        "expansion": "seed[ticker%256,time%15360]; price += (ticker//256)*100 + "
                     "((ticker*17 + (time//240)*31)%101)-50; volume and amount *= "
                     "1+(ticker//256)%7; all four prices use the same shift",
        "profile": "real-seeded dense C; 10000 securities x 64 dates x 240 minutes"})


class Generator:
    def __init__(self, stocks, times):
        root = Path(os.environ["OHLC_BENCH_SEED"])
        self.meta = json.loads((root / "manifest.json").read_text())
        self.seed = np.load(root / "seed.npy", mmap_mode="r")
        self.keys = np.array(self.meta["keys"], dtype="<u4")
        self.stocks = stocks
        self.times = times
        if stocks > 10000 or times > len(self.keys):
            raise ValueError("Workload exceeds the recorded seed parameters")

    def values(self, stocks, times):
        result = self.seed[stocks % self.seed.shape[0], times % self.seed.shape[1]].copy()
        shift = (stocks // 256) * 100 + ((stocks * 17 + (times // 240) * 31) % 101) - 50
        for field in VALUE.names[:4]:
            result[field] += shift
        factor = (1 + (stocks // 256) % 7).astype(np.uint64)
        for field in ("volume", "amount"):
            result[field] *= factor
        return result

    def query(self, kind, stock, first, length):
        if kind == "T":
            return struct.pack("<I", int(self.keys[first]))
        stocks = np.arange(self.stocks, dtype=np.int64) if kind == "X" else np.array(stock)
        times = np.array(first) if kind == "X" else np.arange(first, first + length)
        values = self.values(stocks, times)
        result = np.empty(values.shape, dtype=RESULT)
        result["key"] = stocks if kind == "X" else self.keys[times]
        result["value"] = values
        return struct.pack("<I", len(result)) + result.tobytes()

    def export(self):
        stocks = np.arange(self.stocks, dtype=np.int64)[None, :]
        for first in range(0, self.times, 8):
            times = np.arange(first, min(first + 8, self.times))[:, None]
            values = self.values(stocks, times)
            result = np.empty(values.shape, dtype=WRITE)
            result["ticker"] = stocks
            result["time"] = self.keys[times]
            result["value"] = values
            sys.stdout.buffer.write(result.tobytes())


if __name__ == "__main__":
    if sys.argv[1] == "prepare":
        prepare(Path(sys.argv[2]), Path(sys.argv[3]))
    else:
        generator = Generator(int(sys.argv[2]), int(sys.argv[3]))
        if sys.argv[1] == "export":
            generator.export()
        elif sys.argv[1] == "oracle":
            for line in sys.stdin:
                kind, stock, first, length = line.split()
                sys.stdout.buffer.write(generator.query(kind, int(stock), int(first), int(length)))
                sys.stdout.buffer.flush()
        else:
            raise ValueError("Expected prepare, export, or oracle")
