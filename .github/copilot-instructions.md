# Repository instructions

Read `AGENTS.md` for repository operation rules and `docs/design.md` for product and technical
contracts. The design document is authoritative; rows use its eight-field, nullable 64-byte
format. Use format constants and codecs, not prose comments, to determine encoded sizes.

Keep requirements, formats, algorithms, capacity calculations, and validation criteria in
`docs/design.md`; do not create additional design or research documents. Consult design
section 20 before claiming completed platform, durability, or performance validation. `README.md` and `README_CN.md` are paired user guides.
Keep `LICENSE` as project metadata, not an instruction document.

## Build, test, and format

Run commands from the repository root. Requirements are a C17 compiler, CMake 3.20+, POSIX
threads, system time-zone data, and OpenSSL 1.1.1+ development files for the default network
build. Python 3.9+ enables Python-driven tests; JDK 17+ enables the Java JAR and JNI bridge.

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
ctest --test-dir build --output-on-failure

# Discover enabled tests, or run one named test.
ctest --test-dir build -N
ctest --test-dir build -R '^core$' --output-on-failure

# Build and run only the concurrency executable.
cmake --build build --target ohlc_concurrency_test --parallel
ctest --test-dir build -R '^concurrency$' --output-on-failure

# Check C/header formatting with the pinned clang-format 21.1.8.
python3 tools/check_format.py
```

`tools/requirements.txt` pins the formatter. The checker also accepts its executable path as
the first argument. C compilation treats warnings as errors by default
(`OHLC_WARNINGS_AS_ERRORS=ON`); Java compilation uses `--release 17 -Xlint:all -Werror`.

CTest is the test entry point, including language integration tests: CMake supplies native
library paths and optional Java arguments. Relevant selectors include `core`, `concurrency`,
`pipeline`, `embedded`, `network`, `transport`, `config`, `admin`, and `shell_edit`. Use an
anchored regex to select one or a related group, e.g. `-R '^(network|transport)$'`.
Linux-only `commit_sync_failure` and `wal_reclaim_crash` use libc fault injection through
`LD_PRELOAD`. Available tests depend on platform, build options, Python, and JDK detection;
use `ctest -N` rather than assuming a fixed test count. Unix-socket tests need a short
temporary-directory path.

For C/Python embedded-only work, configure a separate build directory with
`-DOHLC_BUILD_NETWORK=OFF -DOHLC_BUILD_JAVA_CLIENT=OFF`. To keep the Java network client but
omit JNI, use `-DOHLC_BUILD_JAVA_EMBEDDED=OFF`.

```sh
# ASan + UBSan.
cmake -S . -B build-asan -DCMAKE_BUILD_TYPE=Debug -DOHLC_SANITIZE=ON
cmake --build build-asan --parallel
ctest --test-dir build-asan --output-on-failure

# TSan requires a separate build.
cmake -S . -B build-tsan -DCMAKE_BUILD_TYPE=Debug -DOHLC_THREAD_SANITIZE=ON
cmake --build build-tsan --target ohlc_concurrency_test --parallel
ctest --test-dir build-tsan -R '^concurrency$' --output-on-failure
```

Sanitizer builds exclude Python-driven integration and Linux preload tests. Performance
measurements use a separate Release build and the acceptance contracts in design section 19.

## Architecture

`libohlc` is the shared C17 storage engine for both embedded and server access. Public opaque
handles and codecs live in `include/ohlc/ohlc.h`; `src/internal.h` defines shared internal
objects and concurrency ownership. `engine.c` coordinates tables, atomic writes, commit
admission, and snapshot query cursors; `dictionary.c` owns table-local ticker lookup.

Each table is a sparse ticker-by-internal-time matrix. One primary layout serves both series
and cross-section reads: 16 tickers by 8 time codes form an 8 KiB tile, stored ticker-first.
`index.c` maintains copy-on-write roots, a time-key B+ tree, reverse time-code pages, and
band/group directories. `storage.c` owns table volumes, tile metadata, extent allocation,
checksums, and cache; `io.c` owns the optional bounded read pool. `format.c` supplies explicit
little-endian codecs and checksums; `datetime.c` implements the date/time-zone contract.

Writes prepare a candidate root, append WAL, synchronize it, then publish atomic visibility.
Concurrent batches can share one WAL synchronization. Readers pin immutable, reference-counted
roots. `recovery.c` handles WAL replay, two persistent CURRENT checkpoint slots, checkpoint
publication, and reclamation. Table deletion preserves existing query snapshots; physical
files are reclaimed only after both checkpoints cover deletion and relevant cursors close.
`reclaim.c` tracks the extents that checkpoints supersede; later checkpoints reuse their blocks
once neither CURRENT slot nor any live read root references them. Files never shrink.

`ohlcd` exposes the engine through the framed binary protocol over Unix sockets, TCP, or TLS.
`net.c` supplies transport/protocol helpers and `client.c` implements `libohlc_client`.
The `ohlc` shell reuses this C client; parsing, import/export, editing, and help are separated
within the shell modules. `ohlc-admin` provides offline checking, backup, and restore.

Python's `Connection` wraps the C network client through ctypes; `Database` wraps the engine.
The Python package does not build or bundle native libraries: use `OHLC_CLIENT_LIBRARY` and
`OHLC_LIBRARY` to locate them (`.so` on Linux, `.dylib` on macOS). Java's `io.ohlc.Ohlc` speaks
the network protocol directly; `io.ohlc.Database` uses the JNI bridge in `src/java.c`.
Cross-language changes must keep C headers, ctypes declarations, Java/JNI, codecs, and the
server protocol consistent. Current compatibility is disk format 6 / protocol 6 / C ABI 4;
libraries, server, bindings, Java JAR, and JNI must match.

## Contract and code conventions

- Every operation identifies a table. Table IDs belong to a database UUID; ticker and internal
  time codes additionally belong to a table. Cache/isolation keys include table and volume
  ownership. Committed IDs are stable and never reused. Named writes atomically create missing
  tickers; explicit registration is only needed for low-level code-based writes.
- The row fields are `open`, `high`, `low`, `close` (signed 64-bit), `volume`, `amount`,
  `adjust_factor` (unsigned 64-bit), and `reserved` (signed 32-bit), with eight validity bits.
  All fields are nullable. NULL payload bytes and unused flag bits must be zero. Row existence
  has a separate bitmap, so an all-NULL row still exists. Zero is not missing; omitted
  `reserved` becomes NULL. Writes replace the complete row; duplicate keys reject a batch.
- Storage and interfaces are integer-only. Scaling, units, rounding, bar generation, and
  adjustment calculations belong to callers. Native structure sizes are not disk/wire sizes:
  use `OHLC_ROW_BYTES`, `OHLC_WRITE_BYTES`, `OHLC_RESULT_BYTES`, and the codecs. Do not cast
  encoded buffers to naturally aligned structures.
- Users supply dates/date-times, never internal `time_code`. Historical backfill assigns new
  codes without renumbering old ones; series results follow actual `time_key` order. Public
  series ranges are closed `[start, end]`, with both bounds limited to uint32; the wider C/wire
  end parameter detects overflow. Only internal cursors use a widened exclusive endpoint.
- Seconds/minutes require a table time zone; explicit UTC offsets override it. Minute input
  must align to whole minutes. Reject fractional/leap seconds, nonexistent local times, and
  ambiguous local times without an explicit offset. Day/month/year tables preserve full date
  labels without a time zone. Period counts describe supplied bars, not timestamp rounding,
  gap filling, aggregation, or trading-calendar inference. Tickers match exact bytes.
- One process exclusively owns a database directory; shared access goes through the server.
  Core calls are thread-safe except single-owner cursors. Close cursors even after errors and
  before closing the database; follow the public headers' ownership/lifetime contracts.
  Preserve distinct status codes for I/O, corruption, limits, and uncertain commits.
  Never automatically retry `OHLC_OUTCOME_UNKNOWN`.
- Apply design section 18 and `.clang-format`: C17, four spaces, 100-column target, attached
  braces for every control-flow body, `char* p` pointer alignment, and one variable per
  declaration. C symbols use snake_case, public symbols `ohlc_`, constants `OHLC_`, and
  file-private symbols `static`. All code comments, docstrings, Javadoc, and example comments
  are English; design prose and user-facing text may be Chinese.
