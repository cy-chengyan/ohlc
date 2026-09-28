# ohlc

English | [简体中文](README_CN.md)

The ohlc database can only be used to store OHLC market data, focused on two queries:

- Read one ticker over a time range.
- Read every ticker at a single timestamp.

ohlc uses **two-dimensional tiles, computed addressing, fixed-length rows, and no compression**.
It runs as an embedded library or as a standalone server, with C, Python, and Java interfaces
and an interactive command shell.

**Current version: `0.1.0-beta.1` · Deployment target: Linux · License: Apache-2.0**

[Why integer values](#why-integer-values) · [Quick start](#quick-start) ·
[Client libraries](#client-libraries) ·
[RHEL packages](#rhel-packages) · [Benchmarks](#benchmarks) ·
[Design](docs/design.md) · [Test report](docs/test-report.md)

Current development format: **disk 5 / protocol 4 / C ABI 2**. Earlier test databases are
rejected as unsupported; use a new database directory. No migration is provided.

## What it provides

- User-created tables for periods such as `5s`, `3m`, `1d`, `1mo`, and `1y`, all with the same schema.
- **Integer-only market values in both APIs and storage**, exact ticker identifiers, and date or
  date-time input.
- Atomic batches within one table, WAL synchronization before successful write acknowledgment,
  and snapshot-consistent queries.
- Real-time writes, batch import, and complete-row replacement for an existing key.
- Unix sockets, TCP, authenticated TLS, configuration files, and systemd integration.
- A shell with `help`, `series`, `cross`, CSV/TSV import, export, and script execution.
- Statistics, explicit checkpoints, and offline check, backup, and restore tools.

ohlc is specialized for full-bar reads. It does not implement SQL, arbitrary schemas, automatic
aggregation, or a trading calendar. The application supplies the bars, their timestamps, units,
scaling, and adjustment factors.

## Data model

The logical key is **`(table, ticker, timestamp)`**, using a calendar-date label for day/month/year tables.
Every row has these seven fields, in this order:

| Fields | Type |
|---|---|
| `open`, `high`, `low`, `close` | `int32_t` each |
| `volume` | `uint32_t` |
| `amount` | `uint64_t` |
| `adjust_factor` | `uint32_t` |

The encoded row payload is **32 bytes**. Indexes, metadata, WAL, unused tile slots, and retained
checkpoint images require additional space. Native C structure padding is not the storage format;
use the supplied codecs.

Each table owns its own ticker dictionary. Normal insert/write APIs and shell imports create
missing tickers atomically with their rows; no prior registration is needed. IDs start at zero
in each table and are scoped to the database UUID and table ID. Optional table-level registration
is available for low-level writes using persistent codes. IDs remain stable; stopping writes for a delisted ticker preserves its history and does not create
placeholder rows. A stored zero is a valid value, not a missing-data marker.

Range queries use **`[start, end)`** and return records in actual time order, including after historical
backfill. Second/minute tables require a time zone; minute timestamps must align to whole minutes.
An explicit UTC offset overrides the table's zone. Day/month/year tables use full calendar-date
labels. A period such as `5s`, `3m`, `5d`, `1mo`, or `1y`
describes caller-supplied bars: ohlc does not round timestamps, fill gaps, or calculate aggregates.

## Why integer values

**The caller owns precision, units, scaling, rounding, and conversion. ohlc stores and returns the
supplied integers exactly, within each field's declared range.** This boundary applies to all seven
market-data fields in the APIs, shell, imports, and storage. Date and date-time inputs follow the
separate time contract described above.

Keeping integers throughout the interface and storage is a deliberate design choice:

- **Avoid introducing floating-point approximation.** Decimal values such as `123.45` cannot be
  represented exactly in binary floating point. Accepting floats at the API and converting them to
  integers inside the database would already cross that precision boundary.
- **Keep business interpretation with the application.** Markets and data sources use different
  price precision, volume units, amount units, and adjustment-factor scales. ohlc does not choose
  a scale, infer decimal places, or silently round or rescale these values.
- **Preserve the same integer across clients and storage.** Callers can compare the returned integer
  directly with the submitted value without a floating-point tolerance. This is a correctness
  contract, not a claim that integer storage is inherently faster than floating-point storage.

For example, an application that chooses two decimal places converts the price `123.45` to the
integer `12345` before writing. ohlc receives, stores, and returns `12345`; the application renders
it as `123.45` after reading. The factor of `100` is an application convention, not a database
default or an automatically recorded scale.

Callers must keep that convention consistent between writers and readers, choose their conversion
and rounding rules, and ensure the result fits the field's integer range. ohlc cannot recover
precision lost before submission. The authoritative contract is in the
[data contract](docs/design.md#31-七字段定长行).

## Quick start

### Build from source

Requirements:

- A C17 compiler, CMake 3.20+, a build tool, POSIX threads, and system time-zone data.
- OpenSSL development headers and libraries, version 1.1.1 or later, for the default server/client
  build. Use the version maintained by your operating system.
- Python 3.9+ for the Python interfaces and Python-based tests.
- JDK 17+ for the Java JAR and JNI bridge; CMake skips these if a suitable JDK is unavailable.

Clone the repository, then build from its root:

```sh
git clone https://github.com/cy-chengyan/ohlc.git
cd ohlc
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
```

For a C/Python embedded-only build without OpenSSL, add
`-DOHLC_BUILD_NETWORK=OFF -DOHLC_BUILD_JAVA_CLIENT=OFF` when configuring a separate build directory.

### Start the server

```sh
mkdir -p data
./build/ohlcd --data ./data/market --socket /tmp/ohlc-demo.sock
```

Keep this terminal running. In a second terminal, from the repository root:

```sh
./build/ohlc --socket /tmp/ohlc-demo.sock
```

Try the following commands against a fresh demo database:

```text
help;
create bars_1m --period 1m --timezone UTC;
insert bars_1m AAPL "2026-09-01 09:30:00Z" 10000 10100 9950 10080 1200 12100000 1000000;
insert bars_1m AAPL "2026-09-01 09:31:00Z" 10080 10120 10000 10100 900 9100000 1000000;
series bars_1m AAPL from "2026-09-01 09:30:00Z" and "2026-09-01 09:31:00Z" --all;
cross bars_1m "2026-09-01 09:30:00Z" --all;
cross bars_1m "2026-09-01 09:30:00Z" and ticker in ('AAPL', 'GOOGL', 'INTL');
stats;
help import;
quit;
```

`series` returns both bars; `cross` returns the bar at 09:30. `--all` disables the interactive
preview limit. Use `help examples;` for more commands. Stop the server with Ctrl+C in its terminal;
`quit;` only closes the shell. Choose another socket path if the example path is already in use.

The interactive editor supports Ctrl+K/U/W to cut text, Ctrl+Y to paste it back, and Ctrl+R
for incremental reverse history search. Type a search term, press Ctrl+R for an older match,
Enter to submit, Esc to edit the match, or Ctrl+G to restore your original input.
Use `help keys;` for movement, history and other keyboard shortcuts.

Table periods accept a positive count followed by `s`, `m`, `d`, `mo` or `y`:

| Suffix | Bar period | uint32 time key |
|---|---|---|
| `s` | Seconds | Unix seconds |
| `m` | Minutes | Unix minutes |
| `d` | Days | Days since 1970-01-01 |
| `mo` | Months | Days since 1970-01-01; full date label preserved |
| `y` | Years | Days since 1970-01-01; full date label preserved |

```text
create bars_5s --period 5s --timezone Asia/Shanghai;
create bars_1mo --period 1mo;
create bars_1y --period 1y;
```

Second/minute tables require a time zone; date-based tables have none. Second keys cover
1970-01-01T00:00:00Z through 2106-02-07T06:28:15Z, inclusive. Seconds must be `00` for minute
tables; fractional and leap seconds are rejected. Periods never round or aggregate input:
`5s` accepts `09:30:17`, and month/year bars can use any valid date within the time-key range.
Update the server and clients together to use the new units; update Java JAR/JNI together.
The existing row layout and minute/day keys are unchanged.

Shell `series ... from START and END` includes both endpoints; the old `to` syntax is removed.
`cross ... and ticker in (...)` filters on the server, ignores unknown tickers and missing bars,
and returns each match once in table-local ticker-code order. Without the clause, all tickers
are queried. The existing C, Python and Java series APIs keep their half-open ranges.

`drop TABLE;` permanently deletes a table and its rows; see `help drop;`. Existing queries
finish on their original snapshots before files are reclaimed. Recreating the name assigns
a new table ID. C, Python, and Java also provide table deletion in both access modes.
After using table deletion, keep this release or a newer one; older releases cannot read the
deletion records. Existing databases can be opened directly by this release.

## Client libraries

Both modes use the same storage engine and file format. **One process owns a database directory**:
use the server to share it between applications; do not also open its files in embedded mode.

| Language | Server connection | Embedded access |
|---|---|---|
| C | [`libohlc_client`](include/ohlc/client.h) | [`libohlc`](include/ohlc/ohlc.h) |
| Python | `ohlc.Connection` via the C client library | `ohlc.Database` via the C engine |
| Java 17+ | `io.ohlc.Ohlc`, no JNI required | `io.ohlc.Database` via `libohlc_jni` |

### Python

Install the wrapper from this checkout in a virtual environment. The wrapper loads the native
libraries built above; it does not build or bundle them.

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install ./clients/python
export OHLC_CLIENT_LIBRARY="$PWD/build/libohlc_client.so"
export OHLC_LIBRARY="$PWD/build/libohlc.so"
```

These paths are for Linux. On macOS development machines, use `.dylib` instead of `.so`.
After a system installation, the libraries can also be found through the system library search path.

Read from the server and table created in the quick start:

```python
from ohlc import Connection

with Connection(socket="/tmp/ohlc-demo.sock") as client:
    table = client.table("bars_1m")
    with table.series("AAPL", "2026-09-01 09:30:00Z", "2026-09-01 09:32:00Z") as query:
        for chunk in query:
            for time_key, *values in chunk.rows():
                print(table.format_time(time_key), values)
```

Or create a separate database without starting a server:

```python
from ohlc import Database

with Database("./data/python-demo", create=True) as db:
    table = db.create("bars_1m", period="1m", timezone="UTC")
    table.insert("AAPL", "2026-09-01 09:30:00Z",
                 (10000, 10100, 9950, 10080, 1200, 12100000, 1000000))
    with table.cross("2026-09-01 09:30:00Z") as query:
        for chunk in query:
            for row in chunk.rows():
                print(row)
    db.checkpoint()
```

Use a fresh directory for the creation example. For an existing database, open it without
`create=True` and use `db.table(name)`. Embedded applications schedule checkpoints explicitly;
closing a database does not perform one. The server schedules checkpoints in the background.
Successful writes are WAL-durable in both modes.

For batch-oriented applications, use `write()` or `write_encoded()` and consume query chunks rather
than creating a Python object for every row. See the [embedded example](examples/embedded.py) and
[interface and ownership contracts](docs/design.md).

### C and Java

The build produces a C embedded example:

```sh
./build/ohlc_example ./data/c-demo
```

See [examples/embedded.c](examples/embedded.c) for table creation, registration, batched writes,
and both query directions. Installed development files support `pkg-config` packages `ohlc` and
`ohlc-client`, and CMake targets `ohlc::ohlc` and `ohlc::client`.

With JDK 17+ available during the build, run the Java embedded example:

```sh
javac --release 17 -cp build/ohlc-client.jar -d build/examples examples/Embedded.java
java -Djava.library.path="$PWD/build" -cp build/ohlc-client.jar:build/examples \
  Embedded ./data/java-demo
```

Use a separate fresh directory for each example. Java network connections use `Ohlc.unix(path)` or
`Ohlc.connect(...)`; embedded access uses `Database.open(...)`. Use try-with-resources for database,
connection, and query handles. Source: [Ohlc.java](clients/java/io/ohlc/Ohlc.java),
[Database.java](clients/java/io/ohlc/Database.java), [Embedded.java](examples/Embedded.java).

## Configuration and operation

[examples/ohlcd.conf](examples/ohlcd.conf) documents the configuration keys. Edit its data directory,
listener, and resource budgets for your deployment, then validate before starting:

```sh
./build/ohlcd --config /path/to/ohlcd.conf --check-config
./build/ohlcd --config /path/to/ohlcd.conf
```

Command-line options override the file; configuration changes require a restart. Engine memory,
block cache, network buffers, connection count, and query timeouts have separate controls.
The engine memory budget is not a cap on total process RSS or operating-system page cache.
Idle sessions remain connected after the handshake. `timeout-ms` limits handshake and request
I/O, not time spent at the shell prompt. TCP keepalive detects unreachable peers; mutations
with an uncertain outcome are never automatically retried.

Unix sockets are private to their owner by default. TCP supports optional credentials and optional
TLS independently, on both loopback and remote addresses. Omit both credential files for anonymous
read/write access, or configure read or read/write credentials to require authentication.
Omit both `tls-cert` and `tls-key` for plain TCP, or configure both to enable TLS.
Plain TCP transmits data and any credentials without encryption. Credential files must be private.
TLS clients verify the server certificate and identity; they do not fall back to plain TCP.

`ohlc-admin` provides offline `check`, `backup`, and `restore`. Stop the process owning the database
before using these commands. Full configuration, permission, durability, and backup procedures are
in the [design and operations document](docs/design.md).

## RHEL packages

The repository includes [RPM packaging](packaging/ohlc.spec) for RHEL 8 and 9. Source builds do not
configure a package repository; the following commands assume you already have the matching
EL8 or EL9 RPM artifacts and their checksum manifest.

| Package | Contents |
|---|---|
| `ohlc-libs` | Embedded C runtime |
| `ohlc-client-libs` | C network client runtime |
| `ohlc-server` | Daemon, offline administration tool, configuration, and systemd unit |
| `ohlc-client` | Interactive and scriptable shell |
| `ohlc-devel` | C headers, linker symlinks, pkg-config, and CMake metadata |
| `python3-ohlc` | Python network and embedded interfaces |
| `ohlc-java` | Java API JAR |
| `ohlc-jni` | Native bridge for Java embedded access |

From the RPM directory for your system version, verify the manifest and install the server and shell.
For locally built, unsigned beta RPMs, the one-command `--nogpgcheck` option below does not change
the system-wide signature policy.

```sh
sha256sum -c SHA256SUMS
sudo dnf install --nogpgcheck ./ohlc-libs-[0-9]*.rpm ./ohlc-client-libs-[0-9]*.rpm \
  ./ohlc-server-[0-9]*.rpm ./ohlc-client-[0-9]*.rpm
sudo systemctl enable --now ohlc
sudo -u ohlc ohlc --socket /run/ohlc/ohlcd.sock --execute 'help examples;'
```

RPM defaults are `/etc/ohlc/ohlcd.conf`, `/var/lib/ohlc/database`, and a private socket at
`/run/ohlc/ohlcd.sock`. The service runs as the `ohlc` account. Removing the package preserves
database files. Use packages and language bindings from the same release.

## Storage at a glance

The current layout groups **16 tickers × 8 time positions** into a 4 KiB tile. A single primary
tiled layout serves both query directions. Large files hold the tiles; ticker dictionaries, time
maps, and location metadata are resident in memory. Missing records have explicit presence bits.

This favors simple addressing and full-row reads, with a deliberate tradeoff: a partially populated
tile still occupies a whole block. Scattered ticker lifetimes can reduce occupancy and increase
cross-section read amplification, especially when new tickers keep arriving. Checkpoints also
retain older physical images; the current beta does not compact them.

See the [storage diagram](docs/images/ohlc-storage-index-overview.svg),
[addressing diagram](docs/images/ohlc-block-addressing.svg), and
[canonical design](docs/design.md) for the format and algorithms.

## Benchmarks

Measured on **2026-09-27**, using **85,800,000 rows: 10,000 tickers × 8,580 minute bars**,
on RHEL 9.4 x86-64 with NVMe storage and same-host TCP clients. Every timed query receives and
decodes all seven fields. These are configuration-specific measurements, not universal rankings.

| Measurement | ohlc beta.1 | MySQL 8.0.46 | ClickHouse 25.8.31.9 | InfluxDB 2.9.1 OSS |
|---|---:|---:|---:|---:|
| Batch submission, seconds | 19.333 | 443.147 | 100.021 | 1,175.621 |
| 240-row series, warm p50, ms | 0.257 | 0.400 | 2.629 | 5.011 |
| 10,000-row cross-section, warm p50, ms | 4.313 | 5.054 | 59.884 | 421.611 |
| Cross-section after reopen, median, ms | 52.162 | 8.382 | 274.517 | 502.065 |
| Mixed reads, 16 clients, median QPS | 3,792.5 | 5,256.9 | 389.7 | Failed: OOM |

The ohlc batch result is approximately **4.44 million rows/second**. Including ticker registration
and post-import checkpointing, those three phases took 37.60 seconds; initialization and validation
are outside that total. Writes were configured for persistence before acknowledgment.

The host had about 125 GiB RAM, and each service had a 32 GiB cgroup limit. ohlc used a 64 MiB block
cache and a 1 GiB engine budget; MySQL used a 16 GiB buffer pool. The dataset could fit in host memory.
First-query measurements evicted only the test database's file pages before reopening; startup
reloads metadata. Protocols, cache budgets, and storage layouts differ across products.

Bulk ingestion was ohlc's strongest result. MySQL was faster for long-range median latency,
16-client mixed throughput, and the first cross-section after reopening. InfluxDB's failed
16-client run is retained as a failure, not reported as a throughput number.

The [full test report](docs/test-report.md) includes configurations, sample counts, tail latency,
disk and memory measurements, correctness checks, reproduction commands, and 15 additional
retirement/replacement scenarios. Benchmark programs are in [tests/baseline_driver.c](tests/baseline_driver.c)
and [tools/baseline_compare.py](tools/baseline_compare.py). Raw evidence archives are separate
artifacts and are not tracked in Git; the report lists their checksums but does not yet provide
public download links.

## Beta scope

- **RHEL 9 x86-64:** validated on the Linux test server, including package installation and service operation.
- **RHEL 8 x86-64:** tested with UBI 8 userspace and systemd on a RHEL 9 kernel; a standalone
  RHEL 8 kernel has not been validated.
- **Linux ARM64:** source build support is retained; no validated ARM64 release binaries or
  Linux ARM64 runtime acceptance results are provided yet.
- macOS is used for development; Linux is the deployment target.

This beta has no per-row delete, automatic data compaction, replication, clustering, or shared
file access across processes. It stores the caller's adjustment factor without calculating or
carrying it forward. Files may retain historical checkpoint images, so plan disk capacity beyond
32 bytes per record.

The 20-year target scale, sustained production workloads across multiple tables, SELinux enforcing,
FIPS mode, and real power-loss/device-failure behavior require separate validation. Keep the
version-specific [implementation and validation boundaries](docs/design.md) in view when evaluating
the beta for a deployment.

## Development and contributions

Build and run the checks enabled for your platform and installed toolchain:

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

Use a short temporary-directory path when running Unix-socket tests. Linux-only fault-injection
checks and optional language tests depend on the platform and available tools.

Contributions should keep control flow and ownership clear, use C17, English code comments,
four-space indentation, `char* pointer` declarations, and one variable per declaration. Formatting
uses clang-format 21.1.8; see [tools/requirements.txt](tools/requirements.txt) and
[tools/check_format.py](tools/check_format.py). The full coding standards are in
[docs/design.md](docs/design.md).

For bug reports, include the ohlc version, platform, configuration, a minimal reproducer, and the
expected and actual results. Performance reports should also include data distribution, cache state,
result size, and whether they measure server or embedded access. Do not include credentials or
private market data.

The [design document](docs/design.md) is the source of truth for product and technical contracts;
the [test report](docs/test-report.md) records measured results. Both are currently in Chinese.

## License

Apache License 2.0. See [LICENSE](LICENSE).
