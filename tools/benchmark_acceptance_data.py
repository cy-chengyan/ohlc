#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Deterministic quotation workloads and Linux cache/space evidence for acceptance."""

import ctypes
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
import random
import struct
import sys

import numpy as np

from benchmark_quotation import MINUTES, RESULT, VALUE, WRITE


class Workload:
    """Expand the immutable quotation seed; revisions make overwrites independently checkable."""

    def __init__(self, seed, stocks, times):
        self.meta = json.loads((seed / "manifest.json").read_text())
        self.seed = np.load(seed / "seed.npy", mmap_mode="r")
        self.stocks = stocks
        self.times = times
        keys = list(self.meta["keys"][:self.seed.shape[1]])
        day = dt.date.fromisoformat(self.meta["dates"][-1])
        zone = dt.timezone(dt.timedelta(hours=8))
        while len(keys) < times:
            day += dt.timedelta(days=1)
            if day.weekday() < 5:
                keys.extend(int(dt.datetime.fromisoformat(str(day) + " " + minute)
                                .replace(tzinfo=zone).timestamp()) // 60 for minute in MINUTES)
        self.keys = np.array(keys[:times], dtype="<u4")
        if not 1 <= stocks <= 20000 or not 1 <= times <= 1000000:
            raise ValueError("Acceptance workload exceeds its recorded bounds")
        if len(self.keys) > 1 and not np.all(self.keys[1:] > self.keys[:-1]):
            raise ValueError("Workload timestamps must increase strictly")

    def describe(self):
        return {"stocks": self.stocks, "times": self.times,
                "seed_sha256": self.meta["seed_sha256"],
                "keys_sha256": hashlib.sha256(self.keys.tobytes()).hexdigest(),
                "first_key": int(self.keys[0]), "last_key": int(self.keys[-1]),
                "calendar": "First 64 observed seed dates, then explicit generated weekdays",
                "revision_rule": "For revision r>0, prices += r*1048576, volume/amount += r*1024; "
                                 "reserved=r and flags=255; otherwise reserved=NULL and flags=127"}

    def values(self, stocks, times, revisions=0):
        stocks, times, revisions = np.broadcast_arrays(
            np.asarray(stocks, dtype=np.int64), np.asarray(times, dtype=np.int64),
            np.asarray(revisions, dtype=np.int64))
        if (np.any(stocks < 0) or np.any(stocks >= self.stocks) or np.any(times < 0) or
                np.any(times >= self.times) or np.any(revisions < 0) or
                np.any(revisions > 1000000)):
            raise ValueError("Invalid workload coordinates or revision")
        result = self.seed[stocks % 256, times % self.seed.shape[1]].copy()
        shift = stocks // 256 * 100 + (stocks * 17 + times // 240 * 31) % 101 - 50
        for field in VALUE.names[:4]:
            result[field] += shift + revisions * 1048576
        factor = (1 + stocks // 256 % 7).astype(np.uint64)
        for field in ("volume", "amount"):
            result[field] *= factor
            result[field] += (revisions * 1024).astype(np.uint64)
        result["reserved"] = revisions
        result["flags"] = np.where(revisions > 0, 255, 127)
        return result

    def write(self, stocks, times, revisions=0):
        stocks, times, revisions = np.broadcast_arrays(stocks, times, revisions)
        output = np.empty(stocks.shape, dtype=WRITE)
        output["ticker"] = stocks
        output["time"] = self.keys[times]
        output["value"] = self.values(stocks, times, revisions)
        return output.reshape(-1)

    def batch(self, first, count=1, revision=0):
        return self.write(np.arange(self.stocks)[:, None],
                          np.arange(first, first + count)[None, :], revision)

    def expected(self, kind, stock, first, length, revisions=0):
        stocks = np.arange(self.stocks) if kind == "X" else np.array(stock)
        times = np.array(first) if kind == "X" else np.arange(first, first + length)
        output = np.empty(np.broadcast_shapes(stocks.shape, times.shape), dtype=RESULT)
        output["key"] = stocks if kind == "X" else self.keys[times]
        output["value"] = self.values(stocks, times, revisions)
        return output


def space_snapshot(root, stocks=None, times=None, strict=False):
    """Count each inode once and distinguish row layout from retained physical images."""
    totals = {"logical_bytes": 0, "allocated_bytes": 0, "files": 0}
    groups = {}
    seen = set()
    slots = 0
    changing_sizes = []
    removed_paths = []
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise RuntimeError(f"Unexpected symlink in acceptance database: {path}")
        if not path.is_file():
            continue
        try:
            info = path.stat()
        except FileNotFoundError:
            if strict:
                raise
            removed_paths.append(str(path))
            continue
        inode = (info.st_dev, info.st_ino)
        if inode in seen:
            continue
        seen.add(inode)
        if path.name.startswith("data-"):
            kind = "metadata" if path.suffix == ".meta" else "data"
        else:
            kind = path.name.split("-", 1)[0].split(".", 1)[0]
        group = groups.setdefault(kind, {"logical_bytes": 0, "allocated_bytes": 0, "files": 0})
        for item in (totals, group):
            item["logical_bytes"] += info.st_size
            item["allocated_bytes"] += info.st_blocks * 512
            item["files"] += 1
        if kind == "data":
            if info.st_size < 4096 or (info.st_size - 4096) % 8192:
                if strict:
                    raise RuntimeError(f"Unaligned quiescent data volume: {path}: {info.st_size}")
                changing_sizes.append({"path": str(path), "size": info.st_size})
            else:
                slots += (info.st_size - 4096) // 8192
    space = os.statvfs(root)
    result = {**totals, "by_kind": groups, "physical_slots_P": None if changing_sizes else slots,
              "concurrently_changing_volume_sizes": changing_sizes,
              "concurrently_removed_files": removed_paths,
              "filesystem_free_bytes": space.f_bavail * space.f_frsize,
              "hardlinks_deduplicated": True,
              "reflink_note": "st_blocks does not distinguish shared reflink extents"}
    if stocks is not None and times is not None:
        blocks = math.ceil(stocks / 16) * math.ceil(times / 8)
        extents = math.ceil(stocks / 16) * math.ceil(times / 128)
        result["dense_model"] = {"N": stocks * times, "T": times, "Q": blocks, "E": extents,
                                 "row_payload_bytes": 64 * stocks * times,
                                 "live_block_bytes": blocks * 8192,
                                 "retained_or_unreferenced_slots": None if changing_sizes else slots - blocks,
                                 "index_core_model_bytes": None if changing_sizes else
                                                           32 * slots + 16 * extents + 12 * times}
    return result


class FileCache:
    """Use mincore on only this database's data volumes; never drop global host caches."""

    def __init__(self, root):
        self.lib = ctypes.CDLL(None, use_errno=True)
        self.lib.mmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int,
                                 ctypes.c_int, ctypes.c_int, ctypes.c_long]
        self.lib.mmap.restype = ctypes.c_void_p
        self.lib.mincore.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p]
        self.lib.mincore.restype = ctypes.c_int
        self.lib.munmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
        self.lib.munmap.restype = ctypes.c_int
        self.page_size = os.sysconf("SC_PAGE_SIZE")
        self.files = []
        try:
            for path in sorted(root.glob("tables/*/data-*.dat")):
                descriptor = os.open(path, os.O_RDONLY)
                size = os.fstat(descriptor).st_size
                address = self.lib.mmap(None, size, 1, 1, descriptor, 0)
                if address == ctypes.c_void_p(-1).value:
                    os.close(descriptor)
                    raise OSError(ctypes.get_errno(), "mmap")
                pages = math.ceil(size / self.page_size)
                self.files.append((descriptor, size, address, (ctypes.c_ubyte * pages)()))
            if not self.files:
                raise RuntimeError("No data volumes to verify")
        except BaseException:
            self.close()
            raise

    def residency(self):
        resident = 0
        pages = 0
        for descriptor, size, address, vector in self.files:
            if self.lib.mincore(address, size, vector) != 0:
                raise OSError(ctypes.get_errno(), "mincore")
            flags = np.ctypeslib.as_array(vector)
            resident += int(np.count_nonzero(flags & 1))
            pages += len(vector)
        return {"resident_pages": resident, "pages": pages,
                "resident_fraction": resident / pages,
                "resident_bytes": resident * self.page_size,
                "data_volume_bytes": sum(item[1] for item in self.files)}

    def evict(self):
        for descriptor, size, address, vector in self.files:
            os.fsync(descriptor)
            os.posix_fadvise(descriptor, 0, 0, os.POSIX_FADV_DONTNEED)
        result = self.residency()
        if result["resident_fraction"] > .001:
            raise RuntimeError(f"Cold residency exceeds 0.1%: {result}")
        return result

    def warm(self):
        for descriptor, size, address, vector in self.files:
            offset = 0
            while offset < size:
                chunk = os.pread(descriptor, min(8 << 20, size - offset), offset)
                if not chunk:
                    raise RuntimeError("Unexpected data volume EOF")
                offset += len(chunk)
        result = self.residency()
        if result["resident_fraction"] < .99:
            raise RuntimeError(f"Hot residency below 99%: {result}")
        return result

    def close(self):
        for descriptor, size, address, vector in self.files:
            self.lib.munmap(address, size)
            os.close(descriptor)
        self.files.clear()


def verify_workload(seed):
    """Check expanded boundaries against scalar arithmetic and the previous C generator."""
    from benchmark_metrics import sha256
    from benchmark_quotation import Generator

    os.environ["OHLC_BENCH_SEED"] = str(seed)
    old = Generator(10000, 15661)
    expanded = Workload(seed, 20000, 241920)
    if sha256(seed / "seed.npy") != expanded.meta["seed_sha256"]:
        raise RuntimeError("Immutable seed fingerprint differs")
    if not np.array_equal(old.keys[:15661], expanded.keys[:15661]):
        raise RuntimeError("Extended calendar differs from the recorded C dataset")
    rng = random.Random(20261111)
    cases = [(stock, time, revision) for stock in (0, 255, 256, 9999, 10000, 19999)
             for time in (0, 7, 8, 127, 128, 15359, 15660, 241919)
             for revision in (0, 1, 1000000)]
    cases.extend((rng.randrange(20000), rng.randrange(241920), rng.randrange(1000001))
                 for _ in range(1000))
    for stock, time, revision in cases:
        source = [int(value) for value in expanded.seed[stock % 256, time % 15360]]
        shift = stock // 256 * 100 + (stock * 17 + time // 240 * 31) % 101 - 50
        source[:4] = [value + shift + revision * 1048576 for value in source[:4]]
        source[4:6] = [value * (1 + stock // 256 % 7) + revision * 1024 for value in source[4:6]]
        source[7] = revision
        source[8] = 255 if revision else 127
        expected = struct.pack("<IqqqqQQQiI", int(expanded.keys[time]), *source)
        if expanded.expected("S", stock, time, 1, revision).tobytes() != expected:
            raise RuntimeError(f"Expanded scalar/vector mismatch: {(stock, time, revision)}")
    for time in (0, 7, 127, 128, 15359, 15660):
        actual = expanded.expected("X", 0, time, 1)[:10000].tobytes()
        if actual != old.query("X", 0, time, 1)[4:]:
            raise RuntimeError("Extended generator differs from the prior C baseline")
    print(json.dumps({"status": "passed", "scalar_cases": len(cases),
                      "baseline_cross_checks": 6, "seed_sha256": expanded.meta["seed_sha256"],
                      "expanded_workload": expanded.describe()}))


if __name__ == "__main__":
    verify_workload(Path(sys.argv[1]))
