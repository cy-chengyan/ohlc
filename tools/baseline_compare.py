# SPDX-License-Identifier: Apache-2.0
"""Reproduce the edition-1 comparison in a dedicated Linux /ssd02 directory."""

import argparse
import ctypes
import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import random
import secrets
import select
import socket
import stat
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

PORTS = {"ohlc": 18765, "mysql": 13306, "clickhouse": 18123, "influxdb": 18086}
BACKENDS = tuple(PORTS)
FIELDS = ("open", "high", "low", "close", "volume", "amount", "adjust_factor")
BASE_MINUTE = 29803050
SERVER_CPUS = "0-15"
CLIENT_CPUS = "16-23"


def minute_key(minute):
    session, within = divmod(minute, 390)
    week, day = divmod(session, 5)
    return BASE_MINUTE + (week * 7 + day) * 1440 + within


def execute(arguments, **options):
    return subprocess.run([str(value) for value in arguments], check=True,
                          text=True, capture_output=True, **options).stdout


def write_private(path, contents):
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w") as output:
        output.write(contents)


def process_sample(pid):
    directory = Path("/proc") / str(pid)
    fields = (directory / "stat").read_text().rsplit(")", 1)[1].split()
    values = {"cpu_ticks": int(fields[11]) + int(fields[12])}
    for line in (directory / "status").read_text().splitlines():
        name, _, value = line.partition(":")
        if name in ("VmRSS", "VmHWM", "VmSwap"):
            values[name + "_bytes"] = int(value.split()[0]) * 1024
    try:
        for line in (directory / "io").read_text().splitlines():
            name, value = line.split(":", 1)
            values[name] = int(value)
    except PermissionError:
        values["process_io_unavailable"] = True
    return values


class ResourceSampler:
    def __init__(self, experiment, backend):
        self.experiment = experiment
        self.backend = backend
        self.done = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)

    def run(self):
        unit = self.experiment.running[self.backend]
        pid = int(execute(["systemctl", "show", unit, "-p", "MainPID", "--value"]))
        group = execute(["systemctl", "show", unit, "-p", "ControlGroup", "--value"]).strip()
        control = Path("/sys/fs/cgroup") / group.lstrip("/")
        with (self.experiment.results / f"{self.backend}-resources.jsonl").open("a") as output:
            while not self.done.is_set():
                try:
                    value = {"monotonic_ns": time.monotonic_ns(),
                             "stage": self.experiment.stage, **process_sample(pid)}
                    for name in ("memory.current", "memory.peak", "memory.events", "cpu.stat", "io.stat"):
                        path = control / name
                        if path.exists():
                            value[name] = path.read_text().strip()
                    members = []
                    for member in (control / "cgroup.procs").read_text().split():
                        status = (Path("/proc") / member / "status").read_text()
                        record = {"pid": int(member)}
                        for line in status.splitlines():
                            name, _, data = line.partition(":")
                            if name in ("VmRSS", "VmHWM", "VmSwap"):
                                record[name + "_bytes"] = int(data.split()[0]) * 1024
                        members.append(record)
                    value["members"] = members
                    value["rss_sum_bytes"] = sum(item.get("VmRSS_bytes", 0) for item in members)
                    output.write(json.dumps(value) + "\n")
                    output.flush()
                except (FileNotFoundError, ProcessLookupError):
                    break
                self.done.wait(0.5)

    def start(self):
        self.thread.start()

    def stop(self):
        self.done.set()
        self.thread.join(timeout=5)


class QueryDriver:
    def __init__(self, experiment, backend):
        self.errors = (experiment.results / f"{backend}-read.stderr").open("a")
        self.process = subprocess.Popen(experiment.driver_command("read", backend),
                                        env=experiment.driver_environment(), text=True, bufsize=1,
                                        stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=self.errors)
        if self.response().get("event") != "ready":
            raise RuntimeError("Query driver did not become ready")
        self.sequence = 0

    def response(self):
        if not select.select([self.process.stdout], [], [], 180)[0]:
            self.process.kill()
            raise RuntimeError("Query driver timed out")
        line = self.process.stdout.readline()
        if not line:
            raise RuntimeError("Query driver stopped; inspect read.stderr")
        return json.loads(line)

    def query(self, kind, stock, first, length):
        self.sequence += 1
        self.process.stdin.write(f"{kind} {self.sequence} {stock} {first} {length}\n")
        self.process.stdin.flush()
        result = self.response()
        if result.get("id") != self.sequence:
            raise RuntimeError("Query response sequence mismatch")
        return dict(result, kind=kind, stock=stock, first=first, length=length)

    def close(self):
        if self.process.poll() is None:
            self.process.stdin.write("STOP\n")
            self.process.stdin.flush()
        self.process.wait(timeout=10)
        self.errors.close()
        if self.process.returncode != 0:
            raise RuntimeError("Query driver failed")


def data_files(directory):
    for parent, directories, files in os.walk(directory, followlinks=False):
        directories[:] = [name for name in directories if not (Path(parent) / name).is_symlink()]
        for name in files:
            path = Path(parent) / name
            if stat.S_ISREG(path.lstat().st_mode):
                yield path


def evict_files(directory):
    library = ctypes.CDLL(None, use_errno=True)
    library.mmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int,
                            ctypes.c_int, ctypes.c_int, ctypes.c_long]
    library.mmap.restype = ctypes.c_void_p
    library.mincore.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p]
    library.munmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
    page_size = os.sysconf("SC_PAGE_SIZE")
    totals = {"files": 0, "pages": 0, "resident_before": 0, "resident_after": 0}
    for path in data_files(directory):
        size = path.stat().st_size
        if size == 0:
            continue
        with path.open("rb") as source:
            pages = (size + page_size - 1) // page_size
            mapping = library.mmap(None, size, 0, 1, source.fileno(), 0)
            if mapping == ctypes.c_void_p(-1).value:
                raise OSError(ctypes.get_errno(), "mmap failed")
            vector = (ctypes.c_ubyte * pages)()
            try:
                if library.mincore(mapping, size, vector) != 0:
                    raise OSError(ctypes.get_errno(), "mincore failed")
                totals["resident_before"] += sum(value & 1 for value in vector)
                os.posix_fadvise(source.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
                if library.mincore(mapping, size, vector) != 0:
                    raise OSError(ctypes.get_errno(), "mincore failed")
                totals["resident_after"] += sum(value & 1 for value in vector)
            finally:
                library.munmap(mapping, size)
            totals["files"] += 1
            totals["pages"] += pages
    return totals


class Experiment:
    def __init__(self, root, phase, stocks, times):
        self.root = root.resolve()
        if sys.platform != "linux" or Path("/ssd02") not in self.root.parents:
            raise ValueError("Use a dedicated directory under /ssd02 on Linux")
        if self.root.is_symlink() or not 1 <= stocks <= 10000 or not 1 <= times <= 8580:
            raise ValueError("Invalid experiment root or dimensions")
        self.phase = phase
        self.stocks = stocks
        self.times = times
        self.work = self.root / phase
        self.work.mkdir(mode=0o700, exist_ok=True)
        self.results = self.work / "results"
        self.results.mkdir(exist_ok=True)
        self.dataset = self.work / "input"
        self.binary = self.root / "bin/baseline-driver"
        self.events = (self.results / "events.jsonl").open("a", buffering=1)
        self.running = {}
        self.stage = "setup"
        self.unit_prefix = "ohlc-compare-" + hashlib.sha256(str(self.work).encode()).hexdigest()[:10]

    def record(self, event, **values):
        result = dict(event=event, observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                      phase=self.phase, **values)
        self.events.write(json.dumps(result, sort_keys=True) + "\n")
        (self.results / "status.json").write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result, sort_keys=True), flush=True)
        return result

    def token(self):
        return (self.work / "secrets/influx-token").read_text().strip()

    def http(self, backend, path, body=None, *, json_body=None, csv=False, timeout=300):
        headers = {}
        if backend == "influxdb" and (self.work / "secrets/influx-token").exists():
            headers["Authorization"] = "Token " + self.token()
        if json_body is not None:
            body = json.dumps(json_body).encode()
            headers["Content-Type"] = "application/json"
        elif isinstance(body, str):
            body = body.encode()
        if csv:
            headers["Accept"] = "application/csv"
        request = urllib.request.Request(f"http://127.0.0.1:{PORTS[backend]}{path}", data=body,
                                         headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return response.read()
        except urllib.error.HTTPError as error:
            detail = error.read(2000).decode(errors="replace")
            raise RuntimeError(f"{backend} HTTP {error.code}: {detail}") from None

    def mysql(self, sql):
        return execute(["mysql", "--no-defaults", "--protocol=TCP", "-h", "127.0.0.1", "-P",
                        PORTS["mysql"], "-u", "root", "--batch", "--skip-column-names", "-e", sql],
                       timeout=300)

    def clickhouse(self, sql):
        return self.http("clickhouse", "/", sql).decode()

    def influx(self, sql):
        value = self.http("influxdb", "/query?db=baseline&epoch=s", urllib.parse.urlencode({"q": sql}))
        result = json.loads(value)
        if any("error" in item for item in result.get("results", [])):
            raise RuntimeError(result)
        return result

    def prepare_configs(self):
        for backend in BACKENDS:
            (self.work / backend).mkdir(exist_ok=True)
        mysql = self.work / "mysql"
        write_private(self.work / "mysql-init.sql", "CREATE USER IF NOT EXISTS 'root'@'127.0.0.1';\n"
                      "GRANT ALL PRIVILEGES ON *.* TO 'root'@'127.0.0.1';\n")
        write_private(self.work / "mysql.cnf", f"""[mysqld]
datadir={mysql}
init-file={self.work}/mysql-init.sql
socket={mysql}/mysql.sock
port={PORTS['mysql']}
bind-address=127.0.0.1
pid-file={mysql}/mysql.pid
log-error={self.results}/mysql-server.log
skip-networking=OFF
skip-name-resolve
skip-log-bin
local-infile=ON
secure-file-priv=
mysqlx=OFF
innodb_buffer_pool_size=17179869184
innodb_redo_log_capacity=2147483648
innodb_flush_log_at_trx_commit=1
innodb_doublewrite=ON
innodb_flush_method=O_DIRECT
innodb_buffer_pool_dump_at_shutdown=OFF
innodb_buffer_pool_load_at_startup=OFF
max_allowed_packet=67108864
max_connections=128
performance_schema=ON
""")
        clickhouse = self.work / "clickhouse"
        write_private(self.work / "clickhouse.xml", f"""<clickhouse>
  <logger><level>warning</level><log>{self.results}/clickhouse-server.log</log>
    <errorlog>{self.results}/clickhouse-error.log</errorlog><console>0</console></logger>
  <path>{clickhouse}/</path><tmp_path>{clickhouse}/tmp/</tmp_path>
  <listen_host>127.0.0.1</listen_host><http_port>{PORTS['clickhouse']}</http_port>
  <tcp_port>19000</tcp_port><max_connections>128</max_connections>
  <max_server_memory_usage>17179869184</max_server_memory_usage>
  <background_pool_size>16</background_pool_size><background_schedule_pool_size>16</background_schedule_pool_size>
  <mark_cache_size>268435456</mark_cache_size><uncompressed_cache_size>0</uncompressed_cache_size>
  <users_config>{self.work}/clickhouse-users.xml</users_config>
  <default_profile>default</default_profile><default_database>default</default_database>
</clickhouse>
""")
        write_private(self.work / "clickhouse-users.xml", """<clickhouse>
  <profiles><default><max_threads>16</max_threads><max_memory_usage>8589934592</max_memory_usage>
    <use_query_cache>0</use_query_cache><use_query_condition_cache>0</use_query_condition_cache>
    <async_insert>0</async_insert></default></profiles>
  <users><default><password></password><networks><ip>127.0.0.1</ip></networks>
    <profile>default</profile><quota>default</quota></default></users>
  <quotas><default><interval><duration>3600</duration><queries>0</queries><errors>0</errors>
    <result_rows>0</result_rows><read_rows>0</read_rows><execution_time>0</execution_time></interval></default></quotas>
</clickhouse>
""")
        if not (mysql / "auto.cnf").exists():
            output = execute(["/usr/sbin/mysqld", "--no-defaults", "--initialize-insecure",
                              f"--datadir={mysql}", f"--log-error={self.results}/mysql-initialize.log"],
                             timeout=180)
            (self.results / "mysql-initialize.stdout").write_text(output)
        self.record("configured", stocks=self.stocks, times=self.times,
                    rows=self.stocks * self.times, server_cpus=SERVER_CPUS, client_cpus=CLIENT_CPUS,
                    service_memory_max_bytes=32 << 30)

    def command(self, backend):
        directory = self.work / backend
        if backend == "ohlc":
            return [self.root / "build/ohlcd", "--data", directory, "--host", "127.0.0.1",
                    "--port", PORTS[backend], "--memory-mib", 1024, "--cache-mib", 64,
                    "--connections", 64, "--timeout-ms", 120000, "--query-ms", 120000]
        if backend == "mysql":
            return ["/usr/sbin/mysqld", f"--defaults-file={self.work}/mysql.cnf"]
        if backend == "clickhouse":
            return [self.root / "bin/clickhouse", "server", f"--config-file={self.work}/clickhouse.xml"]
        return [self.root / "bin/influxdb2-2.9.1/influxd", "--http-bind-address",
                f"127.0.0.1:{PORTS[backend]}", "--bolt-path", directory / "influxd.bolt",
                "--sqlite-path", directory / "influxd.sqlite", "--engine-path", directory / "engine",
                "--reporting-disabled", "--storage-wal-fsync-delay", "0s",
                "--storage-cache-max-memory-size", 1073741824,
                "--storage-cache-snapshot-memory-size", 67108864,
                "--storage-cache-snapshot-write-cold-duration", "5s",
                "--storage-compact-full-write-cold-duration", "15s"]

    def start(self, backend):
        unit = self.unit_prefix + "-" + backend
        with socket.socket() as probe:
            if probe.connect_ex(("127.0.0.1", PORTS[backend])) == 0:
                raise RuntimeError(f"Port {PORTS[backend]} is already in use")
        started = time.monotonic_ns()
        arguments = ["sudo", "-n", "systemd-run", "--quiet", "--collect", f"--unit={unit}",
                     f"--uid={os.getuid()}", f"--gid={os.getgid()}", "--property=Type=exec",
                     f"--property=AllowedCPUs={SERVER_CPUS}", "--property=MemoryMax=32G",
                     "--property=MemorySwapMax=0", "--property=LimitNOFILE=65536",
                     "--property=TasksMax=4096", "--property=TimeoutStopSec=300",
                     "--setenv=GOMAXPROCS=16", *self.command(backend)]
        execute(arguments)
        self.running[backend] = unit
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            try:
                if backend == "mysql":
                    self.mysql("SELECT 1")
                elif backend == "clickhouse":
                    self.clickhouse("SELECT 1")
                elif backend == "influxdb":
                    self.http(backend, "/health")
                else:
                    with socket.create_connection(("127.0.0.1", PORTS[backend]), timeout=1):
                        pass
                pid = int(execute(["systemctl", "show", unit, "--property=MainPID", "--value"]).strip())
                return self.record("started", backend=backend, pid=pid,
                                   startup_ns=time.monotonic_ns() - started)
            except (RuntimeError, urllib.error.URLError, OSError, subprocess.CalledProcessError):
                state = execute(["systemctl", "show", unit, "--property=ActiveState", "--value"]).strip()
                if state not in ("active", "activating"):
                    break
                time.sleep(0.2)
        self.stop(backend)
        raise RuntimeError(f"{backend} did not become ready; inspect the service journal")

    def stop(self, backend):
        unit = self.running.pop(backend, None)
        if unit is None:
            return
        journal = execute(["sudo", "-n", "journalctl", "-u", unit, "--no-pager", "-n", "500"])
        with (self.results / f"{backend}-journal.log").open("a") as output:
            output.write(journal)
        subprocess.run(["sudo", "-n", "systemctl", "stop", unit], timeout=330,
                       text=True, capture_output=True, check=False)
        self.record("stopped", backend=backend)

    def create_schema(self, backend):
        if backend == "mysql":
            sql = """CREATE DATABASE baseline;
CREATE TABLE baseline.bars (
    ticker INT UNSIGNED NOT NULL, time_key INT UNSIGNED NOT NULL,
    `open` INT NOT NULL, high INT NOT NULL, low INT NOT NULL, `close` INT NOT NULL,
    volume INT UNSIGNED NOT NULL, amount BIGINT UNSIGNED NOT NULL, adjust_factor INT UNSIGNED NOT NULL,
    PRIMARY KEY (ticker, time_key),
    KEY by_time (time_key, ticker, `open`, high, low, `close`, volume, amount, adjust_factor)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;"""
            self.mysql(sql)
        elif backend == "clickhouse":
            self.clickhouse("CREATE DATABASE baseline")
            sql = """CREATE TABLE baseline.bars (
    ticker UInt32, time_key UInt32, open Int32, high Int32, low Int32, close Int32,
    volume UInt32, amount UInt64, adjust_factor UInt32,
    PROJECTION by_time (SELECT * ORDER BY (time_key,ticker))
) ENGINE=MergeTree ORDER BY (ticker,time_key)
SETTINGS index_granularity=256, index_granularity_bytes=0,
         fsync_after_insert=1, fsync_part_directory=1"""
            self.clickhouse(sql)
        elif backend == "influxdb":
            response = json.loads(self.http(backend, "/api/v2/setup", json_body={
                "username": "benchmark", "password": secrets.token_urlsafe(32),
                "org": "ohlc-baseline", "bucket": "bars", "retentionPeriodSeconds": 0}))
            write_private(self.work / "secrets/influx-token", response["auth"]["token"])
            mapping = {"orgID": response["org"]["id"], "bucketID": response["bucket"]["id"],
                       "database": "baseline", "retention_policy": "autogen", "default": True}
            self.http(backend, "/api/v2/dbrps", json_body=mapping)
            sql = json.dumps({"measurement": "bars", "tag": "ticker", "integer_fields": FIELDS,
                              "retention": "infinite", "DBRP": "baseline.autogen"}, indent=2)
        else:
            sql = "Table bars: minute=1, timezone=UTC; ticker dictionary populated by the C driver."
        (self.results / f"{backend}-schema.txt").write_text(sql + "\n")

    def driver_environment(self):
        environment = os.environ.copy()
        if (self.work / "secrets/influx-token").exists():
            environment["OHLC_BENCH_INFLUX_TOKEN"] = self.token()
        return environment

    def driver_command(self, operation, backend, directory=None):
        return ["taskset", "-c", CLIENT_CPUS, str(self.binary), operation, backend,
                str(directory or self.dataset), str(PORTS[backend]), str(self.stocks), str(self.times)]

    def generate(self):
        self.dataset.mkdir(mode=0o700)
        self.record("generation_started")
        output = execute(["taskset", "-c", CLIENT_CPUS, self.binary, "generate", self.dataset,
                          self.stocks, self.times], timeout=1800)
        generated = json.loads(output)
        (self.results / "dataset.json").write_text(json.dumps(generated, indent=2) + "\n")
        self.record("generated", **{key: value for key, value in generated.items() if key != "event"})

    def load(self, backend):
        self.stage = "load"
        self.record("load_started", backend=backend)
        with (self.results / f"{backend}-load.jsonl").open("x") as output:
            with (self.results / f"{backend}-load.stderr").open("x") as errors:
                subprocess.run(self.driver_command("load", backend), env=self.driver_environment(),
                               stdout=output, stderr=errors, check=True, timeout=14400)
        last = json.loads((self.results / f"{backend}-load.jsonl").read_text().splitlines()[-1])
        if last["event"] != "loaded" or last["rows"] != self.stocks * self.times:
            raise RuntimeError("Import did not complete")
        self.record("loaded", backend=backend, **{key: value for key, value in last.items() if key != "event"})

    def settle(self, backend):
        self.stage = "settle"
        started = time.monotonic_ns()
        if backend == "ohlc":
            sys.path.insert(0, str(self.root / "ohlc-0.1.0-beta.1/clients/python"))
            import ohlc
            connection = ohlc.Connection(host="127.0.0.1", port=PORTS[backend],
                                         library=str(self.root / "build/libohlc_client.so"),
                                         timeout_ms=120000)
            try:
                connection.checkpoint()
                (self.results / "ohlc-stats.json").write_text(json.dumps(connection.stats(), indent=2))
            finally:
                connection.close()
        elif backend == "mysql":
            self.mysql("SET GLOBAL innodb_max_dirty_pages_pct=0; "
                       "SET GLOBAL innodb_max_dirty_pages_pct_lwm=0")
            deadline = time.monotonic() + 900
            while True:
                dirty = int(self.mysql("SELECT VARIABLE_VALUE FROM performance_schema.global_status "
                                       "WHERE VARIABLE_NAME='Innodb_buffer_pool_pages_dirty'").strip())
                if dirty == 0:
                    break
                if time.monotonic() >= deadline:
                    raise RuntimeError("MySQL did not finish flushing")
                time.sleep(1)
            self.mysql("SET GLOBAL innodb_max_dirty_pages_pct=75")
        elif backend == "clickhouse":
            self.clickhouse("OPTIMIZE TABLE baseline.bars FINAL")
        else:
            deadline = time.monotonic() + 900
            previous = None
            stable_since = time.monotonic()
            while True:
                current = [(str(path), path.stat().st_size, path.stat().st_mtime_ns)
                           for path in data_files(self.work / backend / "engine")]
                if current != previous:
                    stable_since = time.monotonic()
                    previous = current
                if time.monotonic() - stable_since >= 25:
                    break
                if time.monotonic() >= deadline:
                    raise RuntimeError("InfluxDB files did not become stable")
                time.sleep(1)
            (self.results / "influxdb-settled-metrics.txt").write_bytes(self.http(backend, "/metrics"))
        self.record("settled", backend=backend, wall_ns=time.monotonic_ns() - started)

    def validate(self, backend, full):
        self.stage = "validation"
        expected = json.loads((self.results / "dataset.json").read_text())
        started = time.monotonic_ns()
        if backend == "ohlc":
            client = QueryDriver(self, backend)
            try:
                points = range(self.times) if full else (0, self.times - 1)
                for minute in points:
                    client.query("X", 0, minute, 1)
            finally:
                client.close()
            values = {"verified_rows": self.stocks * (self.times if full else 2),
                      "method": "seven-field oracle for every returned row"}
        else:
            quoted = [(f"`{field}`" if backend == "mysql" else f'"{field}"') for field in FIELDS]
            count = "COUNT(open)" if backend == "influxdb" else "COUNT(*)"
            sql = "SELECT " + count + " AS n," + ",".join(
                f"SUM({field}) AS s{index}" for index, field in enumerate(quoted))
            if backend == "influxdb":
                result = self.influx(sql + " FROM bars")
                series = result["results"][0]["series"][0]
                row = dict(zip(series["columns"], series["values"][0]))
                actual = [row["n"]] + [row[f"s{index}"] for index in range(7)]
            else:
                sql += " FROM baseline.bars"
                result = self.mysql(sql) if backend == "mysql" else self.clickhouse(sql)
                actual = [int(value) for value in result.strip().split("\t")]
            if actual != [expected["rows"], *expected["sums"]]:
                raise RuntimeError(f"{backend} count or seven-field aggregate mismatch: {actual}")
            values = {"verified_rows": actual[0], "sums": actual[1:],
                      "method": "count and seven integer sums; oracle on each timed query"}
        self.record("validated", backend=backend, wall_ns=time.monotonic_ns() - started, **values)

    def warm_reads(self, backend, smoke):
        self.stage = "warm"
        lengths = sorted(set(min(self.times, value) for value in (1, 240, 2048, 8580)))
        plan = [("S", length) for length in lengths] + [("X", 1)]
        randomizer = random.Random(20260927)
        client = QueryDriver(self, backend)
        output = self.results / f"{backend}-warm.jsonl"
        try:
            with output.open("w") as results:
                for round_number in range(3 if not smoke else 1):
                    for kind, length in plan:
                        for index in range(56 if not smoke else 4):
                            stock = randomizer.randrange(self.stocks)
                            first = randomizer.randrange(self.times - length + 1)
                            result = client.query(kind, stock, first, length)
                            result.update(round=round_number, measured=index >= (16 if not smoke else 1))
                            results.write(json.dumps(result) + "\n")
        finally:
            client.close()
        self.record("warm_completed", backend=backend)

    def concurrent_reads(self, backend, smoke):
        self.stage = "concurrency"
        for threads in ((2,) if smoke else (1, 4, 8, 16)):
            for round_number in range(1 if smoke else 3):
                directory = self.results / f"{backend}-stress-{threads}-{round_number}"
                directory.mkdir()
                arguments = self.driver_command("stress", backend, directory)
                arguments.extend((str(threads), "2" if smoke else "15", str(20260927 + round_number)))
                started = time.monotonic_ns()
                with (directory / "summary.json").open("w") as output:
                    with (directory / "stderr.txt").open("w") as errors:
                        subprocess.run(arguments, env=self.driver_environment(), check=True,
                                       timeout=240, stdout=output, stderr=errors)
                self.record("stress_completed", backend=backend, threads=threads,
                            round=round_number, wall_ns=time.monotonic_ns() - started)

    def cold_reads(self, backend):
        self.stage = "first_query_after_reopen"
        randomizer = random.Random(3102026)
        with (self.results / f"{backend}-cold.jsonl").open("w") as output:
            for kind, length in (("S", 240), ("S", self.times), ("X", 1)):
                for repetition in range(5):
                    self.stop(backend)
                    eviction = evict_files(self.work / backend)
                    startup = self.start(backend)
                    client = QueryDriver(self, backend)
                    try:
                        result = client.query(kind, randomizer.randrange(self.stocks),
                                              randomizer.randrange(self.times - length + 1), length)
                    finally:
                        client.close()
                    result.update(repetition=repetition, eviction=eviction,
                                  startup_ns=startup["startup_ns"])
                    output.write(json.dumps(result) + "\n")
                    output.flush()
        self.record("cold_completed", backend=backend)

    def inventory(self, backend):
        self.stage = "inventory"
        if backend == "mysql":
            queries = ("SELECT VERSION()", "SHOW GLOBAL VARIABLES", "SHOW GLOBAL STATUS",
                       "EXPLAIN SELECT time_key,open,high,low,close,volume,amount,adjust_factor "
                       f"FROM baseline.bars WHERE ticker=31 AND time_key BETWEEN {minute_key(0)} "
                       f"AND {minute_key(239)} ORDER BY time_key",
                       "EXPLAIN SELECT ticker,open,high,low,close,volume,amount,adjust_factor "
                       f"FROM baseline.bars WHERE time_key={minute_key(0)}")
            for index, query in enumerate(queries):
                (self.results / f"mysql-inspect-{index}.txt").write_text(self.mysql(query))
        elif backend == "clickhouse":
            queries = ("SELECT version()", "SHOW CREATE TABLE baseline.bars",
                       "SELECT name,value FROM system.merge_tree_settings WHERE name LIKE 'fsync%'",
                       "SELECT * FROM system.parts WHERE database='baseline' FORMAT JSONEachRow",
                       "SELECT * FROM system.projection_parts WHERE database='baseline' FORMAT JSONEachRow",
                       "EXPLAIN indexes=1 SELECT time_key,open,high,low,close,volume,amount,adjust_factor "
                       f"FROM baseline.bars WHERE ticker=31 AND time_key BETWEEN {minute_key(0)} "
                       f"AND {minute_key(239)} ORDER BY time_key",
                       "EXPLAIN indexes=1 SELECT ticker,open,high,low,close,volume,amount,adjust_factor "
                       f"FROM baseline.bars WHERE time_key={minute_key(0)}")
            for index, query in enumerate(queries):
                (self.results / f"clickhouse-inspect-{index}.txt").write_text(self.clickhouse(query))
        elif backend == "influxdb":
            (self.results / "influxdb-final-metrics.txt").write_bytes(self.http(backend, "/metrics"))
            (self.results / "influxdb-series-cardinality.json").write_text(
                json.dumps(self.influx("SHOW SERIES EXACT CARDINALITY"), indent=2))
        sizes = []
        seen = set()
        for path in data_files(self.work / backend):
            info = path.stat()
            identity = (info.st_dev, info.st_ino)
            if identity not in seen:
                sizes.append({"path": str(path.relative_to(self.work / backend)),
                              "logical": info.st_size, "allocated": info.st_blocks * 512})
                seen.add(identity)
        (self.results / f"{backend}-files.json").write_text(json.dumps(sizes, indent=2) + "\n")
        self.record("storage", backend=backend, files=len(sizes),
                    logical_bytes=sum(item["logical"] for item in sizes),
                    allocated_bytes=sum(item["allocated"] for item in sizes))

    def run_backend(self, backend, smoke):
        sampler = None
        try:
            self.start(backend)
            self.create_schema(backend)
            sampler = ResourceSampler(self, backend)
            sampler.start()
            self.load(backend)
            self.settle(backend)
            self.validate(backend, full=not smoke)
            self.warm_reads(backend, smoke)
            self.concurrent_reads(backend, smoke)
            sampler.stop()
            sampler = None
            if not smoke:
                self.cold_reads(backend)
            self.inventory(backend)
            self.stop(backend)
            self.record("backend_completed", backend=backend)
        finally:
            if sampler is not None:
                sampler.stop()
            self.stop(backend)

    def close(self):
        for backend in list(self.running):
            self.stop(backend)
        self.events.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--phase", required=True)
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--backends", nargs="+", choices=BACKENDS, default=list(BACKENDS))
    arguments = parser.parse_args()
    if not arguments.phase.replace("-", "").isalnum():
        parser.error("Use a simple phase name")
    experiment = Experiment(arguments.root, arguments.phase,
                            64 if arguments.smoke else 10000, 512 if arguments.smoke else 8580)
    try:
        experiment.prepare_configs()
        if not (experiment.results / "dataset.json").exists():
            experiment.generate()
        for backend in arguments.backends:
            experiment.run_backend(backend, arguments.smoke)
        experiment.record("completed", backends=arguments.backends)
    except Exception as error:
        experiment.record("failed", error=str(error))
        raise
    finally:
        experiment.close()


if __name__ == "__main__":
    main()
