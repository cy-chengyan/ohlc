#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Refresh the repository report and retrieve verified evidence when home01 finishes."""

import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
import shlex
import subprocess
import time
import traceback

from benchmark_metrics import atomic_json, sha256

REMOTE = "/ssd02/ohlc-acceptance-20261111"
START = "<!-- acceptance-live:start -->"
END = "<!-- acceptance-live:end -->"


def remote(command, **kwargs):
    return subprocess.run(["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "home01", command],
                          check=True, timeout=300, **kwargs)


def remote_json(path):
    return json.loads(remote("sudo -n cat " + shlex.quote(path), capture_output=True, text=True).stdout)


def measurement(workload, event):
    return next((item for item in workload.get("measurements", []) if item["event"] == event), {})


def render(summary, pipeline, finalization):
    timestamp = dt.datetime.fromtimestamp(summary["recorded_at"], dt.timezone(dt.timedelta(hours=8)))
    lines = [f"状态更新时间：{timestamp:%Y-%m-%d %H:%M:%S %z}。", ""]
    stage = pipeline.get("stage", "waiting")
    if stage == "finished":
        if pipeline.get("execution_error") or not pipeline.get("cleanup_passed"):
            lines.append("本轮已有失败或未完成项，不能判为全部通过。")
        else:
            lines.append("本轮测量及正确性核对已完成，清理和源文件复核通过。吞吐不稳定组单独标出；这不等同全部正式 S/C/L 验收。")
    elif stage in ("execution_failed", "finalization_failed", "finalization_deferred"):
        lines.append("执行或收尾存在未完成项，需要处理后才能验收。")
    else:
        lines.append("执行中。尚未完成的项目不记为通过。")
    lines.extend([f"编排阶段：`{stage}`。", ""])
    error = pipeline.get("execution_error") or pipeline.get("error")
    if error:
        detail = error.get("error", str(error)) if isinstance(error, dict) else str(error)
        lines.extend([f"编排记录：`{detail}`。", ""])
    runs = summary.get("runs", {})
    large = runs.get("large01", {})
    post = runs.get("post01", {})
    statuses = {"passed": "完成，核对通过", "unstable": "完成，存在不稳定组",
                "failed": "失败", "incomplete": "未完成"}
    lines.extend(["| 项目 | 当前证据状态 |", "| --- | --- |"])
    for label, run, name in (("L 导入", large, "large-import"),
                             ("L 单读者，read-workers=0", large, "large-natural-0"),
                             ("L 单读者，read-workers=8", large, "large-natural-8"),
                             ("L 并发，read-workers=0", post, "natural-concurrent-0"),
                             ("L 并发，read-workers=8", post, "natural-concurrent-8"),
                             ("24 小时混合读写及全字段核对", post, "mixed")):
        status = run.get("workloads", {}).get(name, {}).get("status", "incomplete")
        lines.append(f"| {label} | {statuses.get(status, status)} |")
    for fault in ("kill", "data", "current"):
        status = post.get("recovery", {}).get(f"recovery-{fault}-result", {}).get("status", "incomplete")
        lines.append(f"| C 库恢复：{fault} | {statuses.get(status, status)} |")
    audit = post.get("audits", {}).get("admin-mixed", {})
    lines.append(f"| 长测停库后完整校验 | {statuses.get(audit.get('status', 'incomplete'), '未完成')} |")
    source = (finalization or {}).get("source_verification") or {}
    lines.extend([f"| 源文件最终复核 | {statuses.get(source.get('status', 'incomplete'), '未完成')} |", ""])
    imported = large.get("workloads", {}).get("large-import", {})
    done = measurement(imported, "import_complete")
    progress = imported.get("last_progress")
    if done:
        lines.extend([f"L 导入 {done['rows']:,} 行；墙钟 {done['wall_ns'] / 1e9:.3f} 秒，"
                      f"写入调用累计 {done['write_ns'] / 1e9:.3f} 秒，"
                      f"最终检查点 {done['settle_ns'] / 1e9:.3f} 秒。", ""])
    elif progress and progress["event"] == "import_batch":
        lines.extend([f"L 已确认导入 {(progress['first'] + progress['times']) * 20000:,} / 4,838,400,000 行。", ""])
    latency = []
    for workers in (0, 8):
        workload = large.get("workloads", {}).get(f"large-natural-{workers}", {})
        if workload.get("status") != "passed":
            continue
        for item in workload.get("measurements", []):
            if item["event"] == "latency_summary":
                values = item["wall_ns"]
                latency.append(f"| {workers} | {item['label']} | {values['samples']} | "
                               f"{values['p50'] / 1e6:.3f} | {values['p95'] / 1e6:.3f} |")
    if latency:
        lines.extend(["L 单读者自然冷变体，延迟单位 ms：", "",
                      "| read-workers | 查询 | 样本 | p50 | p95 |",
                      "| ---: | --- | ---: | ---: | ---: |", *latency, ""])
    throughput = []
    for workers in (0, 8):
        workload = post.get("workloads", {}).get(f"natural-concurrent-{workers}", {})
        selected = {}
        for item in workload.get("measurements", []):
            if item["event"] == "throughput_summary":
                selected[(item["kind"], item["length"], item["readers"])] = item
        for item in selected.values():
            label = "X" if item["kind"] == "X" else "R" + str(item["length"])
            status = "稳定" if item["status"] == "valid" else "不稳定" if item["status"] == "unstable" else "待重测"
            throughput.append(f"| {workers} | {label} | {item['readers']} | "
                              f"{item['mean_qps']:.2f} | {item['cv'] * 100:.2f}% | {status} |")
    if throughput:
        lines.extend(["L 并发自然冷变体，经 TCP 环回：", "",
                      "| read-workers | 查询 | 读者 | QPS | 轮间 CV | 状态 |",
                      "| ---: | --- | ---: | ---: | ---: | --- |", *throughput, ""])
    recovery = post.get("recovery", {})
    if recovery:
        lines.extend(["C 库进程故障恢复：", "",
                      "| 故障 | 新增已确认行 | 未检查点序列数 | WAL 游标后字节 | 启动至监听（ms） |",
                      "| --- | ---: | ---: | ---: | ---: |"])
        for item in recovery.values():
            lines.append(f"| {item['stage']} | {item['ready']['rows']:,} | "
                         f"{item['acknowledged_sequences_after_checkpoint']} | "
                         f"{item['stopped_checkpoint']['wal_bytes_after_checkpoint_cursor']:,} | "
                         f"{item['launch_to_listen_ns'] / 1e6:.3f} |")
        lines.append("")
    mixed = post.get("workloads", {}).get("mixed", {})
    done = measurement(mixed, "mixed_complete")
    progress = mixed.get("last_progress")
    if done:
        values = done["write_latency_ns"]
        lines.extend([f"混合负载持续 {done['elapsed_seconds'] / 3600:.4f} 小时，"
                      f"追加 {done['appends']:,} 批，历史替换 {done['replacements']:,} 批；"
                      f"确认延迟 p50/p95/max 为 {values['p50'] / 1e6:.3f}/"
                      f"{values['p95'] / 1e6:.3f}/{values['max'] / 1e6:.3f} ms。", ""])
    elif progress and progress["event"] == "mixed_progress":
        lines.extend([f"混合负载已运行 {progress['elapsed_seconds'] / 3600:.3f} 小时，"
                      f"追加 {progress['appends']:,} 批、历史替换 {progress['replacements']:,} 批。", ""])
    readers = post.get("readers", {})
    if readers:
        lines.extend(["读者 QPS 为各独立读者的实际请求数/墙钟之和；分钟窗口 p95 范围不是全程 p95。", "",
                      "| 查询 | 基线 QPS | 混合 QPS | 混合分钟窗口 p95 范围（ms） |",
                      "| --- | ---: | ---: | ---: |"])
        for kind in ("S", "X"):
            rates = [0.0, 0.0]
            tails = []
            for reader in readers.values():
                for phase, values in reader["phases"].items():
                    if values["kind"] == kind:
                        rates[int(phase)] += values["qps"]
                tails.extend(window["latency_ns"]["p95"] / 1e6 for window in reader["windows"]
                             if window["kind"] == kind and window["phase"] == 1)
            span = f"{min(tails):.3f}–{max(tails):.3f}" if tails else "待测"
            lines.append(f"| {'R240' if kind == 'S' else 'X'} | {rates[0]:.2f} | {rates[1]:.2f} | {span} |")
        lines.append("")
    states = mixed.get("states", [])
    spaces = [(item["label"], item["space"]) for item in states
              if item["label"] in ("before_baseline", "after_mixed_checkpoint")]
    if audit:
        spaces.append(("停库后", audit["space_before"]))
    if spaces:
        lines.extend(["长测空间快照按 inode 去重；分配量包含文件系统预分配，不扣除 reflink 共享。", "",
                      "| 时点 | 行数 | P | 逻辑 GiB | 分配 GiB |",
                      "| --- | ---: | ---: | ---: | ---: |"])
        for name, space in spaces:
            lines.append(f"| {name} | {space['dense_model']['N']:,} | {space['physical_slots_P']} | "
                         f"{space['logical_bytes'] / 2**30:.3f} | {space['allocated_bytes'] / 2**30:.3f} |")
        lines.append("")
    if audit:
        lines.extend([f"停库校验共核对 {audit['verified_rows']:,} 行，耗时 {audit['wall_ns'] / 1e9:.3f} 秒。", ""])
    archive = post.get("large-archive")
    if archive:
        lines.extend([f"L 库完整校验后归档至 `{archive['archive']}`，SHA-256：`{archive['sha256']}`。"
                      "归档使用的压缩仅用于保存测试文件，不是 OHLC 功能，也不计入数据库空间比较。", ""])
    return "\n".join(lines).rstrip()


def update_report(path, content):
    original = path.read_text()
    if original.count(START) != 1 or original.count(END) != 1:
        raise ValueError("Expected one bounded generated report section")
    prefix, remainder = original.split(START, 1)
    _, suffix = remainder.split(END, 1)
    updated = prefix + START + "\n\n" + content + "\n\n" + END + suffix
    temporary = path.with_suffix(".md.tmp")
    temporary.write_text(updated)
    temporary.replace(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    local = args.root / "run/acceptance-home01-20261111"
    report = args.root / "docs/benchmarks/2026-11-11-home01-acceptance.md"
    while True:
        try:
            pipeline = remote_json(REMOTE + "/results/pipeline-state.json")
            finished = pipeline["stage"] == "finished"
            if pipeline["stage"] == "finalizing":
                time.sleep(30)
                continue
            if not finished:
                script = (f"cd {REMOTE}/source && python3.12 tools/benchmark_acceptance_report.py "
                          f"{REMOTE} --output {REMOTE}/results/stage-summary.json")
                remote("sudo -n -u ohlc taskset -c 16-23 " + shlex.join(["bash", "-c", script]),
                       capture_output=True, text=True)
            summary = remote_json(REMOTE + ("/results/final-summary.json" if finished else "/results/stage-summary.json"))
            finalization = remote_json(REMOTE + "/results/finalization.json") if finished else None
            atomic_json(local / "stage-summary.json", summary)
            atomic_json(local / "pipeline-state.json", pipeline)
            update_report(report, render(summary, pipeline, finalization))
            if finished:
                metadata = remote_json(REMOTE + "/evidence-archive.json")
                archive = local / "evidence.tar.gz"
                with archive.open("wb") as output:
                    remote("sudo -n cat " + shlex.quote(metadata["path"]), stdout=output)
                if archive.stat().st_size != metadata["bytes"] or sha256(archive) != metadata["sha256"]:
                    raise RuntimeError("Retrieved evidence archive fingerprint differs")
                atomic_json(local / "evidence-archive.json", metadata)
                atomic_json(local / "finalization.json", finalization)
                atomic_json(local / "watcher-state.json", {"status": "finished", "time": time.time(),
                                                          "archive_verified": True})
                return
            atomic_json(local / "watcher-state.json", {"status": "watching", "time": time.time(), "pipeline": pipeline})
            if args.once:
                return
        except Exception as error:
            record = {"status": "retrying", "time": time.time(), "error": repr(error),
                      "traceback": traceback.format_exc()}
            atomic_json(local / "watcher-state.json", record)
            print(json.dumps(record), flush=True)
            if args.once:
                raise
        time.sleep(300)


if __name__ == "__main__":
    main()
