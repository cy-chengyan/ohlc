#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Orchestrate isolated quotation comparisons; run this controller as root on home01."""

import argparse
import json
import os
from pathlib import Path
import pwd
import signal
import shutil
import socket
import subprocess
import sys
import time
import traceback

from benchmark_metrics import atomic_json, capture, disk_usage, proc_snapshot, sha256

SYSTEMS = ("ohlc", "clickhouse", "influxdb", "aerospike")
PORTS = {"ohlc": 18765, "clickhouse": 18123, "influxdb": 18086, "aerospike": 13000}


def own(path):
    user = pwd.getpwnam("ohlc")
    os.chown(path, user.pw_uid, user.pw_gid)


def directory(path):
    path.mkdir(parents=True, exist_ok=False)
    own(path)


def service_command(root, name, data, config, installed, smoke):
    if name == "ohlc":
        config.write_text(f"""data={data}
host=127.0.0.1
port=18765
memory-mib=24576
cache-mib=64
network-mib=1024
connections=128
query-ms=300000
timeout-ms=300000
busy-wait-ms=30000
read-workers=0
""")
        return [str(root / "build/ohlcd"), "--config", str(config)]
    return competitor_command(name, data, config, installed, smoke)


def properties(unit):
    raw = subprocess.check_output(["systemctl", "show", unit, "-p", "MainPID", "-p",
                                   "ActiveState", "-p", "Result", "-p", "ControlGroup",
                                   "-p", "MemoryPeak", "-p", "ExecMainStatus"], text=True)
    result = dict(line.split("=", 1) for line in raw.splitlines() if "=" in line)
    relative = result.get("ControlGroup")
    if not relative:
        return result
    group = Path("/sys/fs/cgroup") / relative.lstrip("/")
    for name in ("memory.current", "memory.peak", "memory.events", "memory.stat",
                 "memory.numa_stat", "memory.swap.current", "memory.swap.max",
                 "cpu.stat", "io.stat"):
        path = group / name
        try:
            result[name] = path.read_text().strip()
        except FileNotFoundError:
            # A completed service may release its cgroup during this snapshot.
            continue
    return result


def launch(unit, command, log, cpus, root, memory=None):
    log.touch()
    own(log)
    options = ["systemd-run", "--quiet", "--unit=" + unit,
               "--property=User=ohlc", "--property=Group=ohlc",
               "--property=AllowedCPUs=" + cpus, "--property=CPUAffinity=" + cpus,
               "--property=MemorySwapMax=0", "--property=LimitNOFILE=65536",
               "--property=ReadOnlyPaths=/hd01/quotation",
               "--property=WorkingDirectory=" + str(root / "source"),
               "--property=StandardOutput=append:" + str(log),
               "--property=StandardError=append:" + str(log),
               "--setenv=OHLC_BENCH_SEED=" + str(root / "seed"),
               "--setenv=OPENBLAS_NUM_THREADS=1"]
    if memory:
        options.append("--property=MemoryMax=" + memory)
    subprocess.run(options + command, check=True)


def wait_ready(unit, port):
    for _ in range(180):
        info = properties(unit)
        if info.get("ActiveState") in ("failed", "inactive"):
            raise RuntimeError(f"Service startup failed: {unit}: {info}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=.3):
                return int(info["MainPID"])
        except OSError:
            time.sleep(1)
    raise TimeoutError("Service did not become ready")


def environment(root):
    return {
        "source": json.loads((root / "source/ohlc-benchmark-provenance.json").read_text()),
        "commands": [capture(command) for command in (
            ["uname", "-a"], ["lscpu"], ["lsblk", "-o", "NAME,MODEL,REV,SIZE,MOUNTPOINTS"],
            ["findmnt", "/ssd02"], ["df", "-B1", "/ssd02"], ["free", "-b"],
            ["gcc", "--version"], ["cmake", "--version"], ["openssl", "version"],
            [str(root / "venv/bin/python"), "--version"],
            [str(root / "venv/bin/pip"), "freeze"])],
        "thp": Path("/sys/kernel/mm/transparent_hugepage/enabled").read_text(),
        "governors": {str(path): path.read_text().strip() for path in
                      Path("/sys/devices/system/cpu").glob("cpu*/cpufreq/scaling_governor")},
        "binaries": {path.name: sha256(path) for path in (root / "build").glob("*.so.*")
                     if not path.is_symlink()},
        "server_sha256": sha256(root / "build/ohlcd"),
        "driver_sha256": {path.name: sha256(path) for path in
                          (root / "source/tools").glob("benchmark_*.py")},
        "server_cpus": "0-15", "client_cpus": "24-39", "controller_cpus": "16-23",
        "server_memory_limit": 32 * 1024 ** 3, "uid": pwd.getpwnam("ohlc").pw_uid,
    }


def run_one(args, output, name, manifest):
    root = args.root
    server_unit = "ohlc-quotation-" + output.name + "-" + name
    driver_unit = server_unit + "-client"
    for port in PORTS.values():
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=.2):
                raise RuntimeError(f"A benchmark port is already in use: {port}")
        except (ConnectionRefusedError, TimeoutError):
            pass
    data = output / (name + "-data")
    token = output / "influx.token"
    resume = name == "influxdb" and args.influx_realtime_from is not None
    if resume:
        previous = args.influx_realtime_from.resolve(strict=True)
        if previous.parent != (root / "results").resolve():
            raise ValueError("Recovery input must be an existing run under this benchmark root")
        data = previous / "influxdb-data"
        token = previous / "influx.token"
        if not data.is_dir() or not token.is_file():
            raise ValueError("Missing InfluxDB recovery input")
    else:
        directory(data)
    installed = manifest.get(name, {})
    if name != "ohlc" and (installed.get("status") != "installed" or
                            sha256(installed["binary"]) != installed["binary_sha256"]):
        raise RuntimeError(f"Missing or changed {name} package")
    config = output / (name + (".yml" if name == "influxdb" else ".conf"))
    command = service_command(root, name, data, config, installed, args.smoke)
    if not resume:
        for folder, directories, files in os.walk(data):
            for path in [Path(folder)] + [Path(folder) / item for item in directories + files]:
                own(path)
    own(config)
    server_started = False
    driver_started = False
    result = {"system": name, "status": "invalid"}
    try:
        launch(server_unit, command, output / (name + "-server.log"), "0-15", root, "32G")
        server_started = True
        pid = wait_ready(server_unit, PORTS[name])
        command = [str(root / "venv/bin/python"),
                   str(root / "source/tools/benchmark_competitors.py"), "--system", name,
                   "--binary", str(root / "build/ohlcd"), "--stocks",
                   "32" if args.smoke else "10000", "--times", "480" if args.smoke else "15360",
                   "--server-pid", str(pid), "--data", str(data), "--output",
                   str(output / (name + ".jsonl")), "--token", str(token)]
        if args.smoke:
            command.append("--smoke")
        if resume:
            command.append("--realtime-only")
        launch(driver_unit, command, output / (name + "-client.log"), "24-39", root)
        driver_started = True
        started = time.monotonic()
        with (output / (name + "-monitor.jsonl")).open("w", buffering=1) as monitor:
            while True:
                server = properties(server_unit)
                driver = properties(driver_unit)
                result.update(server=server, driver=driver)
                monitor.write(json.dumps({"time": time.time(), "server": server, "driver": driver,
                                          "server_process": proc_snapshot(pid),
                                          "loadavg": Path("/proc/loadavg").read_text().strip(),
                                          "diskstats": Path("/proc/diskstats").read_text(),
                                          "vmstat": Path("/proc/vmstat").read_text()}) + "\n")
                if driver.get("ActiveState") in ("inactive", "failed"):
                    result.update(driver=driver, server=server)
                    break
                if time.monotonic() - started > 7200:
                    raise TimeoutError("System comparison exceeded two hours")
                if int(server.get("memory.swap.current", "0")):
                    raise RuntimeError("Server cgroup swapped")
                if server.get("ActiveState") != "active":
                    raise RuntimeError("Server stopped during measurement")
                time.sleep(1)
        records = [json.loads(line) for line in (output / (name + ".jsonl")).read_text().splitlines()]
        result["last_record"] = records[-1]
        if (driver.get("ExecMainStatus") == "0" and records[-1].get("event") == "complete"):
            result["status"] = records[-1]["validity"]
        result["space"] = disk_usage(data)
        result["elapsed_seconds"] = time.monotonic() - started
    except Exception as error:
        result.update(error=repr(error), traceback=traceback.format_exc())
    finally:
        for started, unit in ((driver_started, driver_unit), (server_started, server_unit)):
            if started:
                subprocess.run(["systemctl", "stop", unit], check=False)
        atomic_json(output / (name + "-result.json"), result)
    print(json.dumps(result), flush=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--label", required=True)
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--systems", nargs="+", choices=SYSTEMS, default=SYSTEMS)
    parser.add_argument("--influx-realtime-from", type=Path)
    args = parser.parse_args()
    if args.influx_realtime_from is not None and args.systems != ["influxdb"]:
        parser.error("--influx-realtime-from requires --systems influxdb")
    if os.geteuid() != 0:
        raise RuntimeError("Only the controller needs root; all workloads run as ohlc")
    def interrupt(signum, frame):
        raise KeyboardInterrupt(f"Controller received signal {signum}")
    signal.signal(signal.SIGTERM, interrupt)
    os.sched_setaffinity(0, set(range(16, 24)))
    output = args.root / "results" / args.label
    directory(output)
    manifest = json.loads((args.root / "packages/install.json").read_text())
    recorded_environment = environment(args.root)
    atomic_json(output / "environment.json", recorded_environment)
    tool_snapshot = output / "tools"
    directory(tool_snapshot)
    for name, digest in recorded_environment["driver_sha256"].items():
        destination = tool_snapshot / name
        source = args.root / "source/tools" / name
        shutil.copy2(source, destination)
        own(destination)
        if sha256(destination) != digest:
            raise RuntimeError(f"Tool changed while snapshotting: {source}")
    atomic_json(output / "packages.json", manifest)
    siblings = [Path(f"/sys/devices/system/cpu/cpu{cpu}/online") for cpu in
                list(range(48, 64)) + list(range(72, 88))]
    states = {str(path): path.read_text().strip() for path in siblings}
    atomic_json(output / "smt-before.json", states)
    results = []
    try:
        for path in siblings:
            path.write_text("0")
        for name in args.systems:
            results.append(run_one(args, output, name, manifest))
    finally:
        for name, state in states.items():
            Path(name).write_text(state)
        atomic_json(output / "smt-after.json", {str(path): path.read_text().strip()
                                                for path in siblings})
        atomic_json(output / "summary.json", results)


def competitor_command(name, data, config, installed, smoke):
    if name == "clickhouse":
        config.write_text(f"""<clickhouse>
<logger><level>warning</level><console>true</console></logger>
<path>{data}/</path><tmp_path>{data}/tmp/</tmp_path>
<http_port>18123</http_port><listen_host>127.0.0.1</listen_host>
<keep_alive_timeout>3600</keep_alive_timeout>
<max_server_memory_usage>30064771072</max_server_memory_usage>
<background_pool_size>16</background_pool_size>
<background_schedule_pool_size>16</background_schedule_pool_size>
<profiles><default><max_memory_usage>30064771072</max_memory_usage></default></profiles>
<users><default><password></password><networks><ip>127.0.0.1</ip></networks><profile>default</profile>
<quota>default</quota></default></users><quotas><default/></quotas>
</clickhouse>\n""")
        # Keep systemd MainPID on the server itself for process counters.
        command = ["env", "CLICKHOUSE_WATCHDOG_ENABLE=0", installed["binary"],
                   "server", "--config-file=" + str(config)]
    elif name == "influxdb":
        config.write_text(f"""http-bind-address: 127.0.0.1:18086
log-level: warn
reporting-disabled: true
bolt-path: {data}/influxd.bolt
sqlite-path: {data}/influxd.sqlite
engine-path: {data}/engine
storage-wal-fsync-delay: 0s
storage-cache-snapshot-write-cold-duration: 10s
storage-compact-full-write-cold-duration: 60s
""")
        command = ["env", "INFLUXD_CONFIG_PATH=" + str(config), installed["binary"]]
    else:
        (data / "udf/lua").mkdir(parents=True)
        (data / "smd").mkdir()
        config.write_text(f"""mod-lua {{
user-path {data}/udf/lua
}}
service {{
proto-fd-max 15000
cluster-name ohlc-benchmark
work-directory {data}
pidfile {data}/asd.pid
}}
logging {{
console {{
    context any warning
}}
}}
network {{
service {{
    address 127.0.0.1
    port 13000
}}
heartbeat {{
    mode mesh
    port 13002
    interval 150
    timeout 10
}}
fabric {{
    port 13001
}}
admin {{
    address 127.0.0.1
    port 13003
}}
}}
namespace bench {{
replication-factor 1
storage-engine device {{
    file {data}/data.dat
    filesize {"1G" if smoke else "32G"}
    read-page-cache true
}}
}}
""")
        command = [installed["binary"], "--foreground", "--config-file", str(config)]
    return command


if __name__ == "__main__":
    main()
