# SPDX-License-Identifier: Apache-2.0
"""Conservative completion checks for the isolated InfluxDB benchmark store."""

import json
import math
from pathlib import Path
import re


METRICS = ("storage_cache_inuse_bytes", "storage_compactions_active",
           "storage_compactions_queued")


def metric_label(line, name):
    match = re.search(r'(?:\{|,)' + name + r'="((?:\\.|[^"\\])*)"', line)
    return json.loads('"' + match[1] + '"') if match else None


def settled_state(data, raw):
    """Return evidence for a quiet store, or None when completion is unproven.

    InfluxDB 2.7.12 can stop an idle shard's compaction loop before its queued
    gauge is refreshed. Reopened idle shards may never initialize queue series.
    Either case requires empty caches, no active compactions, one TSM file per
    observed shard, no temporary files or tombstones, and empty WAL files. The
    caller must require this exact state to remain unchanged for the full quiet
    interval.
    """
    samples = {name: [] for name in METRICS}
    for line in raw.splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        name = line.split("{", 1)[0].split()[0]
        if name in samples:
            value = float(line.rsplit(None, 1)[1])
            if not math.isfinite(value) or value < 0:
                return None
            samples[name].append((line, value))
    cache, active, queued = (samples[name] for name in METRICS)
    if not cache or any(value for _, value in cache + active):
        return None
    values = {name: sorted(value for _, value in rows) for name, rows in samples.items()}
    if queued and not any(value for _, value in queued):
        return {"validation": "metrics_zero", "settled_metrics": values}
    if data is None:
        return None

    root = Path(data).resolve()
    shard_paths = {metric_label(line, "path") for line, _ in cache}
    if None in shard_paths or len(shard_paths) != len(cache):
        return None
    for line, value in queued:
        if value and (value != 1 or metric_label(line, "level") != "full" or
                      metric_label(line, "path") not in shard_paths):
            return None

    files = []
    try:
        actual_shards = {str(path) for path in (root / "engine/data").glob("*/*/*")
                         if path.is_dir() and path.name.isdecimal() and
                         path.parent.name != "_series"}
        if actual_shards != shard_paths:
            return None
        for name in sorted(shard_paths):
            shard = Path(name)
            if shard.is_symlink() or root not in shard.resolve().parents:
                return None
            entries = list(shard.iterdir())
            if any(path.is_symlink() or ".tmp" in path.name or ".tombstone" in path.name
                   for path in entries):
                return None
            tsm = [path for path in entries if path.suffix == ".tsm" and path.is_file()]
            if len(tsm) != 1:
                return None
            info = tsm[0].stat()
            if info.st_size == 0:
                return None
            files.append({"path": str(tsm[0].relative_to(root)), "bytes": info.st_size,
                          "mtime_ns": info.st_mtime_ns, "inode": info.st_ino})
        wal_files = []
        wal_root = root / "engine/wal"
        if not wal_root.is_dir() or wal_root.is_symlink():
            return None
        for path in sorted(wal_root.rglob("*")):
            if path.is_symlink():
                return None
            if path.is_file():
                if path.stat().st_size:
                    return None
                wal_files.append(str(path.relative_to(root)))
    except FileNotFoundError:
        # A concurrent snapshot or compaction invalidates this observation.
        return None
    return {"validation": "stable_single_tsm_shards", "settled_metrics": values,
            "tsm_files": files, "empty_wal_files": wal_files}
