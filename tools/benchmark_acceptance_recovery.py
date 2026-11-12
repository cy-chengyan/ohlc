#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Persist an external acknowledgement ledger and exercise controlled service crash boundaries."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import time
import traceback

from benchmark_acceptance import Events, Session, record_state
from benchmark_acceptance_data import Workload
from benchmark_metrics import atomic_json, summarize
from ohlc import Error


def write_until_crash(args, log):
    workload = Workload(args.seed, args.stocks, args.times + 60000)
    session = Session(args)
    ledger_path = args.output.parent / (args.output.stem + "-ledger.jsonl")
    values = []
    acknowledged = 0
    started = time.monotonic()
    log("workload", base_times=args.times, duration_seconds=args.duration,
        fault=args.fault, **workload.describe())
    try:
        record_state(args, log, session, "before_crash_writes", args.times)
        with ledger_path.open("x", buffering=1) as ledger:
            while acknowledged < 4 or (args.fault == "kill" and time.monotonic() - started < args.duration):
                first = args.times + acknowledged
                rows = workload.batch(first)
                ledger.write(json.dumps({"event": "intent", "first": first, "rows": len(rows),
                                         "sha256": hashlib.sha256(rows.tobytes()).hexdigest()}) + "\n")
                ledger.flush()
                os.fsync(ledger.fileno())
                start = time.monotonic_ns()
                sequence = session.table.write_encoded(rows)
                elapsed = time.monotonic_ns() - start
                ledger.write(json.dumps({"event": "ack", "first": first, "rows": len(rows),
                                         "sequence": sequence, "wall_ns": elapsed}) + "\n")
                ledger.flush()
                os.fsync(ledger.fileno())
                values.append(elapsed)
                acknowledged += 1
                if acknowledged % 100 == 0:
                    log("write_progress", acknowledged_batches=acknowledged,
                        rows=acknowledged * args.stocks, elapsed_seconds=time.monotonic() - started)
            state = session.owner.stats()
            if args.fault == "kill" and state["commit_seq"] == state["checkpoint_seq"]:
                raise RuntimeError("Stress phase ended without an uncheckpointed acknowledged prefix")
            record_state(args, log, session, "immediately_before_crash", args.times + acknowledged)
            summary = {"acknowledged_batches": acknowledged, "rows": acknowledged * args.stocks,
                       "base_times": args.times, "final_times": args.times + acknowledged,
                       "last_ack_sequence": state["commit_seq"], "engine": state,
                       "write_latency_ns": summarize(values),
                       "write_seconds": time.monotonic() - started, "fault": args.fault}
            atomic_json(args.output.parent / (args.output.stem + "-ready.json"), summary)
            log("ready_for_crash", **summary)
            if args.fault != "kill":
                descriptor = os.open(args.arm, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                os.fsync(descriptor)
                os.close(descriptor)
                try:
                    session.owner.checkpoint()
                except Error as error:
                    log("checkpoint_disconnected", status=error.code)
                else:
                    raise RuntimeError("Injected checkpoint boundary did not terminate the server")
            else:
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    try:
                        session.owner.ping()
                    except Error as error:
                        log("kill_observed", status=error.code)
                        break
                    time.sleep(.02)
                else:
                    raise TimeoutError("Controller did not kill the prepared test service")
    finally:
        session.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--stocks", type=int, default=10000)
    parser.add_argument("--times", type=int, default=15661)
    parser.add_argument("--port", type=int, default=18766)
    parser.add_argument("--mode", default="network", choices=("network",))
    parser.add_argument("--duration", type=float, default=60)
    parser.add_argument("--fault", choices=("kill", "data", "current"), required=True)
    parser.add_argument("--arm", type=Path)
    args = parser.parse_args()
    if args.fault != "kill" and args.arm is None:
        parser.error("Injected checkpoint crashes require an arm path")
    log = Events(args.output)
    try:
        write_until_crash(args, log)
        log("complete", status="passed")
    except BaseException as error:
        log("failed", error=repr(error), traceback=traceback.format_exc())
        raise
    finally:
        log.close()


if __name__ == "__main__":
    main()
