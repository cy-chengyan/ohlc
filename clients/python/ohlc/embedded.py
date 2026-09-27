# SPDX-License-Identifier: Apache-2.0
"""Embedded access to libohlc, with owning chunks and coordinated native lifetimes."""

import ctypes as C
import ctypes.util
from contextlib import contextmanager
from dataclasses import dataclass, fields
import os
import threading
from typing import Optional

from . import Chunk, Error, OutcomeUnknown, Table, _Bytes, _Definition, _Table, _text, _ticker, _uint
from . import _check_abi


class _CoreOptions(C.Structure):
    _fields_ = [("cache_bytes", C.c_size_t), ("memory_limit", C.c_size_t),
                ("data_volume_bytes", C.c_uint64), ("wal_segment_bytes", C.c_uint64),
                ("max_tables", C.c_uint32), ("max_cursors", C.c_uint32),
                ("max_query_ms", C.c_uint32), ("read_workers", C.c_uint32),
                ("create_if_missing", C.c_bool)]


class _Stats(C.Structure):
    _fields_ = [("commit_seq", C.c_uint64), ("checkpoint_seq", C.c_uint64),
                ("ticker_count", C.c_uint64), ("table_count", C.c_uint32),
                ("memory_bytes", C.c_uint64), ("disk_read_bytes", C.c_uint64),
                ("disk_read_calls", C.c_uint64), ("cache_hits", C.c_uint64),
                ("cache_misses", C.c_uint64), ("wal_bytes", C.c_uint64),
                ("data_bytes", C.c_uint64)]


@dataclass(frozen=True)
class DatabaseOptions:
    """Engine budgets in bytes and milliseconds. None uses the native default.

    memory_limit covers engine allocations, including indexes and tile cache;
    it is not a process RSS cap. Call checkpoint() periodically after writes.
    """
    cache_bytes: Optional[int] = None
    memory_limit: Optional[int] = None
    data_volume_bytes: Optional[int] = None
    wal_segment_bytes: Optional[int] = None
    max_tables: Optional[int] = None
    max_cursors: Optional[int] = None
    max_query_ms: Optional[int] = None
    read_workers: Optional[int] = None


def _library(path):
    path = path or os.environ.get("OHLC_LIBRARY") or ctypes.util.find_library("ohlc")
    if not path:
        raise RuntimeError("Set OHLC_LIBRARY to the installed libohlc shared library")
    lib = C.CDLL(os.fspath(path))
    _check_abi(lib)
    pointer = C.c_void_p
    declarations = {
        "ohlc_options_init": ([C.POINTER(_CoreOptions)], None),
        "ohlc_open": ([C.c_char_p, C.POINTER(_CoreOptions), C.POINTER(pointer)], C.c_int),
        "ohlc_close": ([pointer], C.c_int),
        "ohlc_uuid": ([pointer, pointer], None),
        "ohlc_get_stats": ([pointer, C.POINTER(_Stats)], None),
        "ohlc_checkpoint": ([pointer], C.c_int),
        "ohlc_table_open": ([pointer, C.c_char_p, C.POINTER(_Table)], C.c_int),
        "ohlc_table_create": ([pointer, C.POINTER(_Definition), C.POINTER(_Table)], C.c_int),
        "ohlc_table_list": ([pointer, C.c_uint32, C.POINTER(_Table), C.c_size_t,
                             C.POINTER(C.c_size_t), C.POINTER(C.c_uint64)], C.c_int),
        "ohlc_resolve": ([pointer, _Bytes, C.POINTER(C.c_uint32)], C.c_int),
        "ohlc_register": ([pointer, _Bytes, C.POINTER(C.c_uint32), C.POINTER(C.c_uint64)], C.c_int),
        "ohlc_ticker": ([pointer, C.c_uint32, C.POINTER(_Bytes)], C.c_int),
        "ohlc_write": ([pointer, C.c_uint32, pointer, C.c_size_t, C.POINTER(C.c_uint64)], C.c_int),
        "ohlc_series": ([pointer, C.c_uint32, C.c_uint32, C.c_uint32, C.c_uint64,
                         C.POINTER(pointer)], C.c_int),
        "ohlc_cross": ([pointer, C.c_uint32, C.c_uint32, C.POINTER(pointer)], C.c_int),
        "ohlc_cursor_next": ([pointer, pointer, C.c_size_t, C.POINTER(C.c_size_t)], C.c_int),
        "ohlc_cursor_sequence": ([pointer], C.c_uint64),
        "ohlc_cursor_close": ([pointer], None),
        "ohlc_time_parse": ([C.POINTER(_Table), C.c_char_p, C.POINTER(C.c_uint32)], C.c_int),
        "ohlc_time_format": ([C.POINTER(_Table), C.c_uint32, pointer, C.c_size_t], C.c_int),
        "ohlc_status_string": ([C.c_int], C.c_char_p),
    }
    for name, (arguments, result) in declarations.items():
        function = getattr(lib, name)
        function.argtypes = arguments
        function.restype = result
    return lib


class _CursorState:
    def __init__(self, handle):
        self.handle = handle
        self.lock = threading.Lock()


class Database:
    """Own one local database directory. Use a context manager or close().

    Independent operations and cursors may run concurrently; CDLL releases the
    GIL. Close waits for native calls, then closes outstanding cursors. No new
    operations enter during close. The library never retries mutations or
    starts an automatic checkpoint timer. Do not reuse this object after fork.
    """
    max_write_rows = 250000

    def __init__(self, path, *, create=False, options=None, chunk_rows=4096, library=None):
        self._handle = C.c_void_p()
        self._condition = threading.Condition()
        self._active = 0
        self._closing = False
        self._cursors = {}
        self._lib = _library(library)
        self.chunk_rows = _uint(chunk_rows)
        if not 1 <= self.chunk_rows <= 16384:
            raise ValueError("chunk_rows must be 1..16384")
        if not isinstance(create, bool):
            raise TypeError("create must be bool")
        options = DatabaseOptions() if options is None else options
        if not isinstance(options, DatabaseOptions):
            raise TypeError("options must be DatabaseOptions")
        native = _CoreOptions()
        self._lib.ohlc_options_init(C.byref(native))
        widths = {name: C.sizeof(kind) * 8 for name, kind in _CoreOptions._fields_}
        for field in fields(options):
            value = getattr(options, field.name)
            if value is not None:
                setattr(native, field.name, _uint(value, widths[field.name]))
        native.create_if_missing = create
        path = os.fsencode(path)
        if not path or b"\0" in path:
            raise ValueError("Database path must be nonempty and contain no NUL")
        self._check(self._lib.ohlc_open(path, C.byref(native), C.byref(self._handle)))
        uuid = (C.c_ubyte * 16)()
        self._lib.ohlc_uuid(self._handle, uuid)
        self.uuid = bytes(uuid)

    def _check(self, status):
        if status:
            kind = OutcomeUnknown if status == 9 else Error
            raise kind(status, self._lib.ohlc_status_string(status).decode("ascii"))

    @contextmanager
    def _lease(self, optional=False):
        with self._condition:
            handle = self._handle.value if not self._closing else None
            if handle is None and not optional:
                raise Error(5, "Database is closed or closing")
            if handle is not None:
                self._active += 1
        try:
            yield handle
        finally:
            if handle is not None:
                with self._condition:
                    self._active -= 1
                    if self._active == 0:
                        self._condition.notify_all()

    def _close_cursor(self, state):
        if state.handle:
            self._lib.ohlc_cursor_close(state.handle)
            state.handle = None
            with self._condition:
                self._cursors.pop(id(state), None)

    def close(self):
        with self._condition:
            while self._closing:
                self._condition.wait()
            if not self._handle:
                return
            self._closing = True
        try:
            with self._condition:
                while self._active:
                    self._condition.wait()
                cursors = list(self._cursors.values())
            for state in cursors:
                with state.lock:
                    self._close_cursor(state)
            status = self._lib.ohlc_close(self._handle)
            if status == 0:
                self._handle.value = None
            self._check(status)
        finally:
            with self._condition:
                self._closing = False
                self._condition.notify_all()

    def __enter__(self):
        with self._lease():
            return self

    def __exit__(self, *exception):
        self.close()

    def __del__(self):
        if getattr(self, "_handle", None):
            self.close()

    def checkpoint(self):
        """Persist a snapshot and permit safe WAL reclamation; newer writes may follow."""
        with self._lease() as handle:
            self._check(self._lib.ohlc_checkpoint(handle))

    def stats(self):
        result = _Stats()
        with self._lease() as handle:
            self._lib.ohlc_get_stats(handle, C.byref(result))
        return {name: getattr(result, name) for name, _ in _Stats._fields_}

    def table(self, name):
        info = _Table()
        with self._lease() as handle:
            self._check(self._lib.ohlc_table_open(handle, _text(name), C.byref(info)))
        return EmbeddedTable(self, info)

    def create(self, name, *, period="1m", timezone="", description=""):
        if (not isinstance(period, str) or len(period) < 2 or period[-1] not in "md" or
                not period[:-1].isascii() or not period[:-1].isdecimal()):
            raise ValueError("Period must be a positive integer followed by m or d")
        definition = _Definition(_text(name), 1 if period[-1] == "m" else 2,
                                 _uint(int(period[:-1])), _text(timezone), _text(description))
        info = _Table()
        with self._lease() as handle:
            self._check(self._lib.ohlc_table_create(handle, C.byref(definition), C.byref(info)))
        return EmbeddedTable(self, info)

    def tables(self):
        """Iterate table pages; concurrent creations may appear in later pages."""
        start = 1
        while start <= 0xffffffff:
            entries = (_Table * 128)()
            count = C.c_size_t()
            sequence = C.c_uint64()
            with self._lease() as handle:
                self._check(self._lib.ohlc_table_list(handle, start, entries, 128,
                                                     C.byref(count), C.byref(sequence)))
            if not count.value:
                return
            for index in range(count.value):
                table = EmbeddedTable(self, entries[index])
                start = table.id + 1
                yield table

    def _ticker_call(self, ticker, register):
        raw = C.create_string_buffer(_ticker(ticker))
        data = _Bytes(C.addressof(raw), len(raw) - 1)
        code = C.c_uint32()
        sequence = C.c_uint64()
        with self._lease() as handle:
            if register:
                status = self._lib.ohlc_register(handle, data, C.byref(code), C.byref(sequence))
            else:
                status = self._lib.ohlc_resolve(handle, data, C.byref(code))
            self._check(status)
        return (code.value, sequence.value) if register else code.value

    def register(self, ticker):
        return self._ticker_call(ticker, True)

    def resolve(self, ticker):
        return self._ticker_call(ticker, False)

    def dictionary(self):
        count = self.stats()["ticker_count"]
        for code in range(count):
            data = _Bytes()
            with self._lease() as handle:
                self._check(self._lib.ohlc_ticker(handle, code, C.byref(data)))
                ticker = C.string_at(data.data, data.size)
            yield code, ticker

    def _query(self, table, ticker, start, end):
        cursor = C.c_void_p()
        with self._lease() as handle:
            if ticker is None:
                status = self._lib.ohlc_cross(handle, table, start, C.byref(cursor))
            else:
                status = self._lib.ohlc_series(handle, table, ticker, start, end, C.byref(cursor))
            self._check(status)
            try:
                state = _CursorState(cursor)
                result = EmbeddedQuery(self, state)
                with self._condition:
                    self._cursors[id(state)] = state
                return result
            except BaseException:
                if "state" in locals():
                    self._close_cursor(state)
                else:
                    self._lib.ohlc_cursor_close(cursor)
                raise


class EmbeddedTable(Table):
    """Immutable metadata bound to one embedded database; writes are atomic batches."""

    def write_encoded(self, buffer):
        """Borrow a contiguous writable buffer for this call, or copy a readonly buffer.

        Never modify a borrowed buffer until this method returns. Records are
        the same 40-byte little-endian representation used by the C API.
        """
        view = memoryview(buffer).cast("B")
        owner = self.connection
        if not view or len(view) % 40 or len(view) // 40 > owner.max_write_rows:
            raise ValueError("Expected 1..max_write_rows complete 40-byte records")
        storage = ((C.c_ubyte * len(view)).from_buffer_copy(view) if view.readonly
                   else (C.c_ubyte * len(view)).from_buffer(view))
        sequence = C.c_uint64()
        with owner._lease() as handle:
            owner._check(owner._lib.ohlc_write(handle, self.id, storage, len(view) // 40,
                                              C.byref(sequence)))
        return sequence.value

    def series(self, ticker, start, end):
        end = 1 << 32 if end == "@4294967296" or end == 1 << 32 else self.time_key(end)
        return self.connection._query(self.id, self.connection.resolve(ticker),
                                      self.time_key(start), end)

    def cross(self, time):
        return self.connection._query(self.id, None, self.time_key(time), 0)


class EmbeddedQuery:
    """A snapshot cursor. Chunks own their memory; close releases only this cursor.

    Advancing and closing one cursor are serialized. Separate cursors on the
    same database can progress concurrently. An empty final chunk ends iteration.
    """
    def __init__(self, owner, state):
        self._owner = owner
        self._state = state
        self.snapshot_sequence = owner._lib.ohlc_cursor_sequence(state.handle)

    def __iter__(self):
        return self

    def _read_locked(self, storage, capacity):
        owner = self._owner
        count = C.c_size_t()
        with owner._lease():
            status = owner._lib.ohlc_cursor_next(self._state.handle, storage, capacity,
                                                C.byref(count))
            if status or not count.value:
                owner._close_cursor(self._state)
            owner._check(status)
        return count.value

    def read_into(self, buffer):
        """Fill a writable contiguous buffer with at most 16384 result records.

        Return the number of complete 36-byte records, or zero at EOF. Bytes
        after the returned records are unchanged. Reuse the caller-owned
        buffer between calls; do not access it while a read is in progress.
        """
        view = memoryview(buffer).cast("B")
        if view.readonly or len(view) < 36:
            raise ValueError("Expected a writable buffer with room for a result record")
        storage = (C.c_ubyte * len(view)).from_buffer(view)
        with self._state.lock:
            if not self._state.handle:
                return 0
            return self._read_locked(storage, min(16384, len(view) // 36))

    def __next__(self):
        owner = self._owner
        state = self._state
        with state.lock:
            if not state.handle:
                raise StopIteration
            capacity = owner.chunk_rows
            buffer = bytearray(capacity * 36)
            storage = (C.c_ubyte * len(buffer)).from_buffer(buffer)
            count = self._read_locked(storage, capacity)
            return Chunk(memoryview(buffer)[:count * 36].toreadonly(), count,
                         self.snapshot_sequence, count == 0)

    def close(self):
        with self._state.lock:
            with self._owner._lease(optional=True) as handle:
                if handle is not None:
                    self._owner._close_cursor(self._state)

    def __enter__(self):
        return self

    def __exit__(self, *exception):
        self.close()

    def __del__(self):
        if getattr(self, "_state", None) is not None:
            self.close()
