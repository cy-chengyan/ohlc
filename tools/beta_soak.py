# SPDX-License-Identifier: Apache-2.0
"""Run a bounded mixed workload and verify every row again after reopening."""

import argparse
from pathlib import Path
import signal
import sys

from benchmark_linux import Recorder, Worker, disk_usage, load_stage, start_server
from validate_performance import stress


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binaries", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--seconds", type=int, default=600)
    arguments = parser.parse_args()
    output = arguments.output.resolve()
    if sys.platform != "linux" or Path("/ssd02") not in output.parents:
        parser.error("Use a fresh output directory under /ssd02 on Linux")
    if not 30 <= arguments.seconds <= 600:
        parser.error("Duration must be 30..600 seconds")
    binaries = arguments.binaries.resolve()
    output.mkdir(mode=0o700)
    results = output / "results"
    results.mkdir()
    recorder = Recorder(results)
    database = output / "database"
    stocks = 10000
    history = 512
    try:
        recorder.record("configuration", stocks=stocks, history=history, readers=8,
                        write_batches=arguments.seconds, rows_per_batch=stocks,
                        write_interval_ms=1000)
        load_stage(binaries / "ohlc_workload", database, stocks, 0, history, recorder)
        server, log, endpoint = start_server(binaries, database, recorder, "tcp", 64, "beta")
        worker = None
        try:
            worker = Worker(binaries / "ohlc_workload", endpoint, stocks, history, recorder,
                            "beta", server.pid)
            worker.send(f"W {history} {arguments.seconds} 1000 0")
            stress(worker, recorder, 1, "beta", 8, arguments.seconds * 1000)
            if not worker.writer_done:
                worker.wait("writer_complete")
        finally:
            if worker is not None:
                worker.close()
            server.send_signal(signal.SIGTERM)
            try:
                server.wait(timeout=120)
            finally:
                if server.poll() is None:
                    server.kill()
                    server.wait()
                log.close()
            if server.returncode != 0:
                raise RuntimeError(f"Server stopped with status {server.returncode}")
        total_times = history + arguments.seconds
        worker = Worker(binaries / "ohlc_workload", database, stocks, total_times, recorder,
                        "reopened")
        rows = 0
        try:
            for key in range(total_times):
                value = worker.query(key, "X", 0, key, 1)
                rows += value["rows"]
            worker.query(total_times, "S", stocks - 1, 0, total_times)
        finally:
            worker.close()
        if rows != stocks * total_times:
            raise RuntimeError("Reopened row count mismatch")
        recorder.progress("complete", reopened_verified_rows=rows,
                          database=disk_usage(database))
    finally:
        recorder.output.close()


if __name__ == "__main__":
    main()
