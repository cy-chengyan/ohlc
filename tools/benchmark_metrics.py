# SPDX-License-Identifier: Apache-2.0
"""Linux measurements shared by the quotation benchmark."""

import hashlib
import json
import math
import os
from pathlib import Path
import subprocess

GIB = 1 << 30
PAGE_SIZE = os.sysconf("SC_PAGE_SIZE")
TICKS = os.sysconf("SC_CLK_TCK")
THROUGHPUT_WARMUP_SECONDS = 1
THROUGHPUT_ROUND_SECONDS = 5
LATENCY_CAP_SECONDS = 120


def proc_snapshot(pid):
    """Read process counters without inserting work into the engine's locks."""
    try:
        root = Path("/proc") / str(pid)
        fields = (root / "stat").read_text().rsplit(") ", 1)[1].split()
        counters = {}
        for line in (root / "io").read_text().splitlines():
            name, value = line.split(":", 1)
            counters[name] = int(value)
        counters["cpu_ns"] = (int(fields[11]) + int(fields[12])) * 1_000_000_000 // TICKS
        counters["rss_bytes"] = int(fields[21]) * PAGE_SIZE
        for line in (root / "status").read_text().splitlines():
            if line.startswith("VmHWM:"):
                counters["peak_rss_bytes"] = int(line.split()[1]) * 1024
            elif line.startswith("VmSwap:"):
                counters["swap_bytes"] = int(line.split()[1]) * 1024
        return counters
    except (FileNotFoundError, ProcessLookupError, PermissionError) as error:
        # Linux may revoke /proc/<pid>/io access while a process becomes a zombie.
        return {"counters_unavailable": type(error).__name__}


def cgroup_swap_snapshot(pid):
    """Require the measured process's v2 cgroup to forbid swapping."""
    entries = Path(f"/proc/{pid}/cgroup").read_text().splitlines()
    relative = next((line[3:] for line in entries if line.startswith("0::")), None)
    if relative is None:
        raise RuntimeError("Driver needs a cgroup v2 memory.swap.max limit")
    root = Path("/sys/fs/cgroup") / relative.lstrip("/")
    limit = (root / "memory.swap.max").read_text().strip()
    if limit != "0":
        raise RuntimeError("Driver cgroup must set MemorySwapMax=0")
    result = {"path": relative, "max": limit,
              "current_bytes": int((root / "memory.swap.current").read_text()),
              "events": (root / "memory.swap.events").read_text()}
    peak = root / "memory.swap.peak"
    if peak.exists():
        result["peak_bytes"] = int(peak.read_text())
    return result


def process_delta(before, after):
    return {key: after[key] - before[key]
            for key in ("read_bytes", "write_bytes", "rchar", "wchar", "cpu_ns")
            if key in before and key in after}


def disk_usage(root, budget_gib=None, reserve_gib=64):
    result = {"logical_bytes": 0, "allocated_bytes": 0, "by_kind": {}}
    for directory, directories, files in os.walk(root):
        for name in directories + files:
            path = Path(directory) / name
            if path.is_symlink() and root.resolve() not in path.resolve().parents:
                raise RuntimeError("A benchmark symlink escapes its owned data directory")
        for name in files:
            info = (Path(directory) / name).lstat()
            result["logical_bytes"] += info.st_size
            result["allocated_bytes"] += info.st_blocks * 512
            kind = name.split("-", 1)[0] if "-" in name else name
            result["by_kind"][kind] = result["by_kind"].get(kind, 0) + info.st_size
    space = os.statvfs(root)
    result["filesystem_free_bytes"] = space.f_bavail * space.f_frsize
    if ((budget_gib is not None and result["allocated_bytes"] > budget_gib * GIB) or
            result["filesystem_free_bytes"] < reserve_gib * GIB):
        raise RuntimeError("Benchmark disk budget reached")
    return result


def atomic_json(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w") as output:
        json.dump(value, output, indent=2, sort_keys=True)
        output.write("\n")
        output.flush()
        os.fsync(output.fileno())
    temporary.replace(path)
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def capture(command):
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    return {"command": command, "returncode": result.returncode,
            "stdout": result.stdout, "stderr": result.stderr}


def summarize(values):
    if not values:
        raise ValueError("No samples")
    ordered = sorted(values)
    result = {"samples": len(values), "p50": ordered[math.ceil(.5 * len(values)) - 1],
              "p95": ordered[math.ceil(.95 * len(values)) - 1], "max": ordered[-1]}
    if len(values) >= 1000:
        result["p99"] = ordered[math.ceil(.99 * len(values)) - 1]
    return result
