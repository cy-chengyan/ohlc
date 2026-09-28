# SPDX-License-Identifier: Apache-2.0
"""Batch-oriented network and embedded interfaces to the ohlc storage engine."""

import ctypes as C
import ctypes.util
import os
import struct
import threading
from collections import OrderedDict
from dataclasses import dataclass

__version__ = "0.1.0b1"

__all__ = ["Connection", "Table", "Chunk", "Error", "OutcomeUnknown",
           "Database", "DatabaseOptions", "EmbeddedTable", "EmbeddedQuery"]


class Error(Exception):
    def __init__(self, code, message):
        self.code = code
        super().__init__(message)


class OutcomeUnknown(Error):
    """A mutation may have committed. Inspect its result; never blindly retry."""


_PERIOD_UNITS = {"m": 1, "d": 2, "s": 3, "mo": 4, "y": 5}
_PERIOD_SUFFIXES = {unit: suffix for suffix, unit in _PERIOD_UNITS.items()}


def _period(value):
    if not isinstance(value, str):
        raise ValueError("Period must be a positive integer followed by s, m, d, mo or y")
    for suffix, unit in _PERIOD_UNITS.items():
        if not value.endswith(suffix):
            continue
        digits = value[:-len(suffix)]
        if not digits or not digits.isascii() or not digits.isdecimal():
            break
        count = _uint(int(digits))
        if count == 0:
            break
        return unit, count
    raise ValueError("Period must be a positive integer followed by s, m, d, mo or y")


class _Bytes(C.Structure):
    _fields_ = [("data", C.c_void_p), ("size", C.c_size_t)]


class _Options(C.Structure):
    _fields_ = [("socket_path", C.c_char_p), ("host", C.c_char_p),
                ("port", C.c_char_p), ("ca_file", C.c_char_p), ("token", _Bytes),
                ("timeout_ms", C.c_uint32), ("tls", C.c_bool)]


class _Info(C.Structure):
    _fields_ = [("uuid", C.c_ubyte * 16), ("max_frame", C.c_uint32),
                ("max_rows", C.c_uint32), ("query_ms", C.c_uint32),
                ("capabilities", C.c_uint32)]


class _Table(C.Structure):
    _fields_ = [("id", C.c_uint32), ("created_seq", C.c_uint64), ("unit", C.c_int),
                ("period", C.c_uint32), ("name", C.c_char * 64),
                ("timezone", C.c_char * 256), ("description", C.c_char * 4097)]


class _Definition(C.Structure):
    _fields_ = [("name", C.c_char_p), ("unit", C.c_int), ("period", C.c_uint32),
                ("timezone", C.c_char_p), ("description", C.c_char_p)]


def _uint(value, bits=32):
    if not isinstance(value, int) or isinstance(value, bool) or not 0 <= value < 1 << bits:
        raise ValueError(f"Expected uint{bits}: {value!r}")
    return value


def _text(value):
    if not isinstance(value, str) or "\x00" in value:
        raise ValueError("Expected text without NUL")
    return value.encode("utf-8")


def _ticker(value):
    if not isinstance(value, (str, bytes, bytearray, memoryview)):
        raise TypeError("Ticker must be text or bytes")
    result = value.encode("utf-8") if isinstance(value, str) else bytes(value)
    if not 1 <= len(result) <= 4096:
        raise ValueError("Ticker must contain 1..4096 bytes")
    return result


def _check_abi(lib):
    try:
        version = lib.ohlc_abi_version
    except AttributeError as error:
        raise RuntimeError("The native library predates the supported ohlc ABI") from error
    version.argtypes = []
    version.restype = C.c_uint32
    if version() != 2:
        raise RuntimeError("Incompatible ohlc native ABI; install matching libraries and bindings")


def _library(path):
    path = path or os.environ.get("OHLC_CLIENT_LIBRARY") or ctypes.util.find_library("ohlc_client")
    if not path:
        raise RuntimeError("Set OHLC_CLIENT_LIBRARY to the installed libohlc_client shared library")
    lib = C.CDLL(path)
    _check_abi(lib)
    pointer = C.c_void_p
    declarations = {
        "ohlc_connection_options_init": ([C.POINTER(_Options)], None),
        "ohlc_client_connect": ([C.POINTER(_Options), C.POINTER(pointer)], C.c_int),
        "ohlc_client_close": ([pointer], None),
        "ohlc_client_info": ([pointer, C.POINTER(_Info)], None),
        "ohlc_client_call": ([pointer, C.c_uint16, pointer, C.c_size_t, C.POINTER(_Bytes)], C.c_int),
        "ohlc_client_table_open": ([pointer, C.c_char_p, C.POINTER(_Table)], C.c_int),
        "ohlc_client_table_create": ([pointer, C.POINTER(_Definition), C.POINTER(_Table)], C.c_int),
        "ohlc_client_resolve": ([pointer, C.c_uint32, _Bytes, C.POINTER(C.c_uint32)], C.c_int),
        "ohlc_client_register": ([pointer, C.c_uint32, _Bytes, C.POINTER(C.c_uint32),
                                  C.POINTER(C.c_uint64)], C.c_int),
        "ohlc_client_write_named": ([pointer, C.c_uint32, C.POINTER(_Bytes), C.c_size_t,
                                      pointer, C.c_size_t, C.POINTER(C.c_uint64)], C.c_int),
        "ohlc_client_write": ([pointer, C.c_uint32, pointer, C.c_size_t, C.POINTER(C.c_uint64)], C.c_int),
        "ohlc_client_series": ([pointer, C.c_uint32, C.c_uint32, C.c_uint32, C.c_uint64], C.c_int),
        "ohlc_client_cross": ([pointer, C.c_uint32, C.c_uint32], C.c_int),
        "ohlc_client_next": ([pointer, C.POINTER(_Bytes), C.POINTER(C.c_uint32),
                              C.POINTER(C.c_uint64), C.POINTER(C.c_bool)], C.c_int),
        "ohlc_time_parse": ([C.POINTER(_Table), C.c_char_p, C.POINTER(C.c_uint32)], C.c_int),
        "ohlc_time_format": ([C.POINTER(_Table), C.c_uint32, pointer, C.c_size_t], C.c_int),
        "ohlc_status_string": ([C.c_int], C.c_char_p),
    }
    for name, (arguments, result) in declarations.items():
        function = getattr(lib, name)
        function.argtypes = arguments
        function.restype = result
    return lib


class Connection:
    """One connection, one request at a time. Context exit always closes it.

    ctypes CDLL releases the GIL during blocking C calls. A per-connection lock
    prevents concurrent native calls and close/read races. Independent
    connections provide parallelism. There is no implicit reconnection/retry.
    """

    def __init__(self, *, socket=None, host="127.0.0.1", port=8765, tls=False,
                 ca_file=None, token=b"", timeout_ms=30000, library=None, cache_entries=65536):
        self._lib = _library(library)
        self._lock = threading.RLock()
        self._handle = C.c_void_p()
        self._cache = OrderedDict()
        self._cache_entries = _uint(cache_entries)
        self._cache_bytes = 0
        options = _Options()
        self._lib.ohlc_connection_options_init(C.byref(options))
        options.socket_path = _text(os.fspath(socket)) if socket is not None else None
        options.host = _text(host)
        options.port = str(_uint(port, 16)).encode()
        options.tls = tls
        options.ca_file = _text(os.fspath(ca_file)) if ca_file else None
        options.timeout_ms = _uint(timeout_ms)
        token = bytes(token)
        token_buffer = C.create_string_buffer(token)
        options.token = _Bytes(C.addressof(token_buffer), len(token))
        self._check(self._lib.ohlc_client_connect(C.byref(options), C.byref(self._handle)))
        self._info = _Info()
        self._lib.ohlc_client_info(self._handle, C.byref(self._info))
        self.uuid = bytes(self._info.uuid)
        self.max_write_rows = self._info.max_rows
        self.capabilities = self._info.capabilities

    def _check(self, code):
        if code:
            kind = OutcomeUnknown if code == 9 else Error
            raise kind(code, self._lib.ohlc_status_string(code).decode("ascii"))

    def _require_open(self):
        if not self._handle:
            raise Error(5, "Connection is closed")

    def _call(self, opcode, body=b""):
        with self._lock:
            self._require_open()
            response = _Bytes()
            buffer = C.create_string_buffer(body)
            self._check(self._lib.ohlc_client_call(self._handle, opcode, buffer, len(body),
                                                 C.byref(response)))
            return C.string_at(response.data, response.size)

    def ping(self):
        if self._call(2):
            self.close()
            raise Error(6, "Unexpected PING response")

    def stats(self):
        """Return engine counters; byte counters measure I/O, not live disk usage."""
        body = self._call(12)
        if len(body) != 88:
            self.close()
            raise Error(6, "Unexpected STATS response")
        names = ("commit_seq", "checkpoint_seq", "ticker_count", "table_count", "memory_bytes",
                 "disk_read_bytes", "disk_read_calls", "cache_hits", "cache_misses", "wal_bytes",
                 "data_bytes")
        result = dict(zip(names, struct.unpack("<11Q", body)))
        if result["table_count"] > 0xffffffff:
            self.close()
            raise Error(6, "Invalid STATS table count")
        return result

    def checkpoint(self):
        """Persist a committed snapshot. Requires write access; never retries."""
        if self._call(13):
            self.close()
            raise OutcomeUnknown(9, "Unexpected CHECKPOINT acknowledgement")

    def close(self):
        with self._lock:
            if self._handle:
                self._lib.ohlc_client_close(self._handle)
                self._handle = C.c_void_p()
                self._cache.clear()
                self._cache_bytes = 0

    def __enter__(self):
        self._require_open()
        return self

    def __exit__(self, *exception):
        self.close()

    def __del__(self):
        if getattr(self, "_handle", None):
            self.close()

    def table(self, name):
        with self._lock:
            self._require_open()
            info = _Table()
            self._check(self._lib.ohlc_client_table_open(self._handle, _text(name), C.byref(info)))
            return Table(self, info)

    def create(self, name, *, period="1m", timezone="", description=""):
        unit, count = _period(period)
        definition = _Definition(_text(name), unit, count, _text(timezone), _text(description))
        with self._lock:
            self._require_open()
            info = _Table()
            self._check(self._lib.ohlc_client_table_create(self._handle, C.byref(definition),
                                                         C.byref(info)))
            return Table(self, info)

    def drop(self, name):
        """Permanently delete the resolved table. Never retries an uncertain write."""
        with self._lock:
            table = self.table(name)
            response = self._call(14, struct.pack("<I", table.id))
            if len(response) != 8 or struct.unpack("<Q", response)[0] == 0:
                self.close()
                raise OutcomeUnknown(9, "Malformed table deletion acknowledgement")
            for key in list(self._cache):
                if key[0] == table.id:
                    self._cache_bytes -= len(key[1]) + 128
                    del self._cache[key]
            return struct.unpack("<Q", response)[0]

    def _ticker_call(self, table, ticker, register):
        ticker = _ticker(ticker)
        key = (_uint(table), ticker)
        with self._lock:
            self._require_open()
            if not register and key in self._cache:
                self._cache.move_to_end(key)
                return self._cache[key]
            buffer = C.create_string_buffer(ticker)
            data = _Bytes(C.addressof(buffer), len(ticker))
            code = C.c_uint32()
            sequence = C.c_uint64()
            if register:
                status = self._lib.ohlc_client_register(self._handle, _uint(table), data, C.byref(code),
                                                       C.byref(sequence))
            else:
                status = self._lib.ohlc_client_resolve(self._handle, _uint(table), data, C.byref(code))
            self._check(status)
            if self._cache_entries:
                if key not in self._cache:
                    self._cache_bytes += len(ticker) + 128
                self._cache[key] = code.value
                self._cache.move_to_end(key)
                while len(self._cache) > self._cache_entries or self._cache_bytes > 16 * 1024 * 1024:
                    evicted, _ = self._cache.popitem(last=False)
                    self._cache_bytes -= len(evicted[1]) + 128
            return (code.value, sequence.value) if register else code.value

    def register(self, table, ticker):
        return self._ticker_call(table, ticker, True)

    def resolve(self, table, ticker):
        return self._ticker_call(table, ticker, False)

    def dictionary(self, table):
        start = 0
        while start <= 0xffffffff:
            body = self._call(4, struct.pack("<III", _uint(table), start, 128))
            reader = _Reader(body)
            reader.unpack("<Q")
            count, = reader.unpack("<I")
            if count > 128:
                raise Error(6, "Invalid dictionary count")
            entries = []
            for _ in range(count):
                code, = reader.unpack("<I")
                ticker = reader.string()
                if code < start or not 1 <= len(ticker) <= 4096:
                    raise Error(6, "Invalid dictionary entry")
                entries.append((code, ticker))
                start = code + 1
            reader.finish()
            yield from entries
            if not count:
                return

    def tables(self):
        start = 1
        while start <= 0xffffffff:
            reader = _Reader(self._call(11, struct.pack("<II", start, 128)))
            reader.unpack("<Q")
            count, = reader.unpack("<I")
            if count > 128:
                raise Error(6, "Invalid table count")
            entries = []
            for _ in range(count):
                info = _Table()
                info.id, info.created_seq = reader.unpack("<IQ")
                name = reader.string()
                info.unit, info.period = reader.unpack("<II")
                zone = reader.string()
                description = reader.string()
                if (info.id < start or not 1 <= len(name) <= 63 or len(zone) > 255 or
                        len(description) > 4096 or b"\0" in name + zone + description):
                    raise Error(6, "Invalid table definition")
                info.name = name
                info.timezone = zone
                info.description = description
                entries.append(Table(self, info))
                start = info.id + 1
            reader.finish()
            yield from entries
            if not count:
                return


class _Reader:
    def __init__(self, data):
        self.data = data
        self.position = 0

    def unpack(self, format):
        size = struct.calcsize(format)
        if size > len(self.data) - self.position:
            raise Error(6, "Truncated response")
        result = struct.unpack_from(format, self.data, self.position)
        self.position += size
        return result

    def string(self):
        size, = self.unpack("<I")
        if size > len(self.data) - self.position:
            raise Error(6, "Truncated string")
        result = self.data[self.position:self.position + size]
        self.position += size
        return result

    def finish(self):
        if self.position != len(self.data):
            raise Error(6, "Unexpected response suffix")


class Table:
    """Immutable table metadata bound to its originating connection and UUID."""

    def __init__(self, connection, info):
        self.connection = connection
        self._info = info
        self.id = info.id
        self.name = info.name.decode("utf-8")
        self.timezone = info.timezone.decode("ascii")
        self.description = info.description.decode("utf-8")
        if info.unit not in _PERIOD_SUFFIXES:
            raise Error(7, "Unsupported period unit")
        self.period = f"{info.period}{_PERIOD_SUFFIXES[info.unit]}"
        self.created_seq = info.created_seq

    def resolve(self, ticker):
        return self.connection.resolve(self.id, ticker)

    def register(self, ticker):
        """Optionally reserve a table-local code; normal writes do not require this."""
        return self.connection.register(self.id, ticker)

    def dictionary(self):
        return self.connection.dictionary(self.id)

    def _write_named(self, tickers, encoded):
        storage = (C.c_ubyte * len(encoded)).from_buffer(encoded)
        buffers = [C.create_string_buffer(ticker) for ticker in tickers]
        names = (_Bytes * len(buffers))(*[
            _Bytes(C.addressof(buffer), len(buffer) - 1) for buffer in buffers])
        sequence = C.c_uint64()
        with self.connection._lock:
            self.connection._require_open()
            self.connection._check(self.connection._lib.ohlc_client_write_named(
                self.connection._handle, self.id, names, len(names), storage,
                len(encoded) // 40, C.byref(sequence)))
        return sequence.value

    def time_key(self, value):
        if isinstance(value, int):
            return _uint(value)
        if hasattr(value, "isoformat"):
            value = value.isoformat()
        key = C.c_uint32()
        self.connection._check(self.connection._lib.ohlc_time_parse(
            C.byref(self._info), _text(value), C.byref(key)))
        return key.value

    def format_time(self, key):
        output = C.create_string_buffer(64)
        self.connection._check(self.connection._lib.ohlc_time_format(
            C.byref(self._info), _uint(key), output, len(output)))
        return output.value.decode("ascii")

    def write_encoded(self, buffer):
        """Commit a contiguous buffer of little-endian (code, time, row) records."""
        view = memoryview(buffer).cast("B")
        if not view or len(view) % 40 or len(view) // 40 > self.connection.max_write_rows:
            raise ValueError("Expected 1..max_write_rows complete 40-byte records")
        storage = ((C.c_ubyte * len(view)).from_buffer_copy(view) if view.readonly
                   else (C.c_ubyte * len(view)).from_buffer(view))
        sequence = C.c_uint64()
        with self.connection._lock:
            self.connection._require_open()
            self.connection._check(self.connection._lib.ohlc_client_write(
                self.connection._handle, self.id, storage, len(view) // 40, C.byref(sequence)))
        return sequence.value

    def write(self, rows):
        """Commit one batch of (ticker, date_or_datetime, seven_integer_values).

        Missing names are created atomically with the rows. A rejected batch
        creates no names. The method never splits a transaction.
        """
        encoded = bytearray()
        names = {}
        named_size = 4
        for ticker, time, values in rows:
            ticker = _ticker(ticker)
            if ticker not in names:
                names[ticker] = len(names)
                named_size += 4 + len(ticker)
            if named_size + len(encoded) + 40 > (16 << 20) - 104:
                raise ValueError("Named batch exceeds frame limit")
            if len(encoded) // 40 >= self.connection.max_write_rows:
                raise ValueError("Batch exceeds max_write_rows")
            if len(values) != 7:
                raise ValueError("Expected open high low close volume amount adjust_factor")
            for value in values:
                if not isinstance(value, int) or isinstance(value, bool):
                    raise ValueError("All seven fields must be integers")
            try:
                encoded.extend(struct.pack("<IIiiiiIQI", names[ticker],
                                           self.time_key(time), *values))
            except struct.error as error:
                raise ValueError(str(error)) from error
        if not encoded:
            raise ValueError("Expected a nonempty batch")
        return self._write_named(list(names), encoded)

    def insert(self, ticker, time, values):
        return self.write([(ticker, time, values)])

    def series(self, ticker, start, end):
        end = 1 << 32 if end == "@4294967296" or end == 1 << 32 else self.time_key(end)
        with self.connection._lock:
            self.connection._require_open()
            code = self.resolve(ticker)
            self.connection._check(self.connection._lib.ohlc_client_series(
                self.connection._handle, self.id, code, self.time_key(start), end))
        return _Query(self.connection)

    def cross(self, time):
        with self.connection._lock:
            self.connection._require_open()
            self.connection._check(self.connection._lib.ohlc_client_cross(
                self.connection._handle, self.id, self.time_key(time)))
        return _Query(self.connection)


@dataclass(frozen=True)
class Chunk:
    """An owning memoryview; safe to retain after advancing or closing a query."""
    data: memoryview
    count: int
    snapshot_sequence: int
    final: bool

    def rows(self):
        return struct.iter_unpack("<IiiiiIQI", self.data)


class _Query:
    def __init__(self, connection):
        self.connection = connection
        self.complete = False

    def __iter__(self):
        return self

    def __next__(self):
        if self.complete:
            raise StopIteration
        data = _Bytes()
        count = C.c_uint32()
        sequence = C.c_uint64()
        final = C.c_bool()
        with self.connection._lock:
            self.connection._require_open()
            status = self.connection._lib.ohlc_client_next(
                self.connection._handle, C.byref(data), C.byref(count), C.byref(sequence),
                C.byref(final))
            if status:
                self.connection.close()
                self.connection._check(status)
            self.complete = final.value
            # One owning copy per chunk avoids dangling native views without
            # allocating a Python object for every market-data row.
            buffer = memoryview(C.string_at(data.data, data.size))
        return Chunk(buffer, count.value, sequence.value, final.value)

    def close(self):
        if not self.complete:
            self.connection.close()
            self.complete = True

    def __enter__(self):
        return self

    def __exit__(self, *exception):
        self.close()

    def __del__(self):
        self.close()


# Load definitions after the shared codecs; native libraries are opened lazily.
from .embedded import Database, DatabaseOptions, EmbeddedTable, EmbeddedQuery
