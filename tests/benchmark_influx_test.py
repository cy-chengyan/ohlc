#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Check compaction evidence and the full quiet interval without a live server."""

from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
from benchmark_competitors import InfluxDB
from benchmark_influx import settled_state


class InfluxCompletionTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.shard = self.root / "engine/data/bucket/autogen/1"
        self.shard.mkdir(parents=True)
        self.tsm = self.shard / "000000050-000000002.tsm"
        self.tsm.write_bytes(b"finished TSM fixture")
        self.wal = self.root / "engine/wal/bucket/autogen/1/000000051.wal"
        self.wal.parent.mkdir(parents=True)
        self.wal.touch()
        (self.root / "engine/data/bucket/_series/00").mkdir(parents=True)

    def metrics(self, cache=0, active=0, queued=1, level="full"):
        labels = f'{{path="{self.shard}",id="1"}}'
        queue_labels = labels[:-1] + f',level="{level}"' + "}"
        return (f"storage_cache_inuse_bytes{labels} {cache}\n"
                f"storage_compactions_active{labels} {active}\n"
                f"storage_compactions_queued{queue_labels} {queued}\n")

    def test_stale_full_gauge_requires_file_evidence(self):
        state = settled_state(self.root, self.metrics())
        self.assertEqual(state["validation"], "stable_single_tsm_shards")
        self.assertEqual(len(state["tsm_files"]), 1)
        self.assertIsNone(settled_state(None, self.metrics()))

    def test_multiple_tsm_files_are_not_complete(self):
        (self.shard / "000000051-000000001.tsm").write_bytes(b"new snapshot")
        self.assertIsNone(settled_state(self.root, self.metrics()))

    def test_tombstones_and_temporary_files_block_completion(self):
        for name in ("000000050-000000002.tsm.tombstone", "000000051-000000002.tsm.tmp"):
            with self.subTest(name=name):
                path = self.shard / name
                path.touch()
                self.assertIsNone(settled_state(self.root, self.metrics()))
                path.unlink()

    def test_wal_and_active_work_block_completion(self):
        self.wal.write_bytes(b"pending write")
        self.assertIsNone(settled_state(self.root, self.metrics()))
        self.wal.write_bytes(b"")
        for metrics in (self.metrics(cache=1), self.metrics(active=1),
                        self.metrics(queued=2), self.metrics(level="1")):
            with self.subTest(metrics=metrics):
                self.assertIsNone(settled_state(self.root, metrics))

    def test_missing_or_mismatched_observations_block_completion(self):
        self.assertIsNone(settled_state(self.root, ""))
        (self.shard.parent / "2").mkdir()
        self.assertIsNone(settled_state(self.root, self.metrics()))

    def test_zero_metrics_keep_the_existing_completion_path(self):
        state = settled_state(None, self.metrics(queued=0))
        self.assertEqual(state["validation"], "metrics_zero")

    def test_uninitialized_queue_series_require_file_evidence(self):
        raw = self.metrics().splitlines()[0]
        self.assertIsNone(settled_state(None, raw))
        state = settled_state(self.root, raw)
        self.assertEqual(state["validation"], "stable_single_tsm_shards")
        self.tsm.unlink()
        self.assertIsNone(settled_state(self.root, raw))

    def test_file_change_restarts_the_full_quiet_interval(self):
        driver = InfluxDB.__new__(InfluxDB)
        driver.data = self.root
        driver.call = lambda *args, **kwargs: self.metrics().encode()
        now = [0.0]

        def sleep(seconds):
            now[0] += seconds
            if now[0] == 64:
                self.tsm.write_bytes(b"a later compacted TSM fixture")

        with patch("benchmark_competitors.time.monotonic", side_effect=lambda: now[0]), \
                patch("benchmark_competitors.time.sleep", side_effect=sleep):
            result = driver.settle()
        self.assertEqual(now[0], 129)
        self.assertEqual(result["validation"], "stable_single_tsm_shards")


if __name__ == "__main__":
    unittest.main()
