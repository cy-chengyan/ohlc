#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Print compact progress without dumping raw latency arrays."""

import json
from pathlib import Path
import sys


root = Path(sys.argv[1])
for folder in sorted((root / "results").iterdir()):
    if not folder.is_dir():
        continue
    print(folder.name)
    for name in ("ohlc", "clickhouse", "influxdb", "aerospike"):
        path = folder / (name + ".jsonl")
        if not path.exists():
            continue
        rows = 0
        last = None
        summaries = []
        for line in path.read_text().splitlines():
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            if item["event"] == "import_batch":
                rows += item["rows"]
            if item["event"] in ("import_complete", "latency_summary", "throughput_summary",
                                 "realtime_summary", "invalid", "complete"):
                summaries.append(item)
            last = {key: item[key] for key in ("event", "label", "readers", "kind", "sample")
                    if key in item}
        print(name, "imported", rows, "last", last)
        if summaries:
            print(json.dumps(summaries[-1]))
    for path in folder.glob("*-result.json"):
        value = json.loads(path.read_text())
        print(path.name, value.get("status"), value.get("error", ""))
