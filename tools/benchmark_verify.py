#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Verify immutable source fingerprints and scalar/vector workload agreement."""

import json
import os
from pathlib import Path
import random
import struct
import sys

from benchmark_metrics import sha256
from benchmark_quotation import Generator


def verify(root, source=False):
    manifest = json.loads((root / "manifest.json").read_text())
    if sha256(root / "seed.npy") != manifest["seed_sha256"]:
        raise RuntimeError("Seed fingerprint changed")
    checked = 0
    if source:
        for name, original in manifest["sources"].items():
            path = Path(name)
            info = path.stat()
            if (info.st_size != original["size"] or info.st_mtime_ns != original["mtime_ns"] or
                    sha256(path) != original["sha256"]):
                raise RuntimeError(f"Immutable source changed: {name}")
            checked += 1
    os.environ["OHLC_BENCH_SEED"] = str(root)
    generator = Generator(10000, 15661)
    seed = generator.seed
    for field in seed.dtype.names[:4]:
        if int(seed[field].min()) - 50 < -(1 << 63) or int(seed[field].max()) + 3950 >= 1 << 63:
            raise ValueError("Expanded price could overflow")
    for field in ("volume", "amount"):
        if int(seed[field].max()) * 7 >= 1 << 64:
            raise ValueError("Expanded unsigned field could overflow")
    rng = random.Random(194201)
    cases = [(stock, first) for stock in (0, 255, 256, 9999)
             for first in (0, 239, 240, 15359, 15360, 15660)]
    cases.extend((rng.randrange(10000), rng.randrange(15661)) for _ in range(1000))
    for stock, first in cases:
        raw = generator.query("S", stock, first, 1)
        count, key, *values = struct.unpack("<IIqqqqQQQiI", raw)
        expected = [int(value) for value in seed[stock % 256, first % 15360]]
        offset = stock // 256 * 100 + (stock * 17 + first // 240 * 31) % 101 - 50
        expected[:4] = [value + offset for value in expected[:4]]
        expected[4] *= 1 + (stock // 256) % 7
        expected[5] *= 1 + (stock // 256) % 7
        if count != 1 or key != manifest["keys"][first] or values != expected:
            raise ValueError(f"Scalar/vector disagreement: {(stock, first)}")
    for first in (0, 239, 240, 15359, 15360):
        raw = generator.query("X", 0, first, 1)
        if struct.unpack_from("<I", raw)[0] != 10000:
            raise ValueError("Wrong cross-section length")
        for stock in (0, 255, 256, 9999):
            row = struct.unpack_from("<IqqqqQQQiI", raw, 4 + 68 * stock)
            series = struct.unpack("<IIqqqqQQQiI", generator.query("S", stock, first, 1))
            if row[0] != stock or row[1:] != series[2:]:
                raise ValueError("Series/cross-section disagreement")
    print(json.dumps({"status": "passed", "scalar_cases": len(cases),
                      "cross_checks": 20, "source_files_verified": checked,
                      "seed_sha256": manifest["seed_sha256"]}))


if __name__ == "__main__":
    verify(Path(sys.argv[1]), source="--source" in sys.argv[2:])
