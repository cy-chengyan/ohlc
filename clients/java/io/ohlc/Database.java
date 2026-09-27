// SPDX-License-Identifier: Apache-2.0
package io.ohlc;

import java.io.IOException;
import java.lang.ref.Cleaner;
import java.lang.ref.Reference;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** Java 17 embedded access to libohlc. A database exclusively owns its directory.
 * Independent calls and cursors can run concurrently. Use try-with-resources;
 * close waits for native calls and releases every outstanding cursor. Cleaner
 * is a fallback, not a checkpoint timer. Schedule checkpoint() after writes.
 * All uint32 values use long; uint64 counters preserve their bits in long.
 * Native errors are Ohlc.Failure with the C status code. Invalid Java arguments
 * throw IllegalArgumentException. Mutations are never retried automatically.
 */
public final class Database implements AutoCloseable {
    private static final Cleaner CLEANER = Cleaner.create();
    public static final int MAX_WRITE_ROWS = 250000;
    private final State state;
    private final Cleaner.Cleanable cleanable;

    private static final class Native {
        static {
            String path = System.getProperty("ohlc.jni.library");
            if (path == null) {
                System.loadLibrary("ohlc_jni");
            } else {
                System.load(Path.of(path).toAbsolutePath().toString());
            }
            if (abiVersion() != 1) {
                throw new UnsatisfiedLinkError("Incompatible ohlc native ABI");
            }
        }
        private static native int abiVersion();
        private static native long[] defaults();
        private static native long open(byte[] path, long[] options, boolean create)
            throws Ohlc.Failure;
        private static native int close(long database);
        private static native byte[] uuid(long database);
        private static native long[] stats(long database);
        private static native void checkpoint(long database) throws Ohlc.Failure;
        private static native byte[] table(long database, byte[] name, long id) throws Ohlc.Failure;
        private static native byte[] create(long database, byte[] name, long period, boolean days,
                                           byte[] timezone, byte[] description) throws Ohlc.Failure;
        private static native long[] ticker(long database, byte[] ticker, boolean register)
            throws Ohlc.Failure;
        private static native byte[] tickerAt(long database, long code) throws Ohlc.Failure;
        private static native long write(long database, long table, ByteBuffer rows, int offset,
                                         int length) throws Ohlc.Failure;
        private static native long series(long database, long table, long ticker, long start,
                                          long end) throws Ohlc.Failure;
        private static native long cross(long database, long table, long time) throws Ohlc.Failure;
        private static native long sequence(long cursor);
        private static native int next(long cursor, ByteBuffer target, int offset, int rows)
            throws Ohlc.Failure;
        private static native void closeCursor(long cursor);
        private static native long parse(boolean days, byte[] timezone, byte[] time)
            throws Ohlc.Failure;
        private static native String format(boolean days, long key) throws Ohlc.Failure;
    }

    /** Byte and millisecond budgets; defaults are obtained from the native core.
     * memoryLimit includes indexes and cache, not all JVM/process allocations.
     */
    public record Options(long cacheBytes, long memoryLimit, long dataVolumeBytes,
                          long walSegmentBytes, long maxTables, long maxCursors,
                          long queryMs, int readWorkers) {
        public Options {
            if (cacheBytes < 0 || memoryLimit <= 0 || dataVolumeBytes <= 0 || walSegmentBytes <= 0
                    || maxTables <= 0 || maxCursors <= 0 || queryMs <= 0 || readWorkers < 0) {
                throw new IllegalArgumentException("Invalid engine budgets");
            }
            Ohlc.uint32(maxTables);
            Ohlc.uint32(maxCursors);
            Ohlc.uint32(queryMs);
        }

        public static Options defaults() {
            long[] values = Native.defaults();
            return new Options(values[0], values[1], values[2], values[3], values[4], values[5],
                               values[6], (int) values[7]);
        }

        private long[] values() {
            return new long[] {cacheBytes, memoryLimit, dataVolumeBytes, walSegmentBytes,
                               maxTables, maxCursors, queryMs, readWorkers};
        }
    }

    public record Stats(long commitSequence, long checkpointSequence, long tickerCount,
                        long tableCount, long memoryBytes, long diskReadBytes, long diskReadCalls,
                        long cacheHits, long cacheMisses, long walBytes, long dataBytes) {}

    /* Cleanup state must not reference the Java owner: doing so would keep it
     * reachable forever through Cleaner. Cursor cleanup states are owned here. */
    private static final class State implements Runnable {
        final ReentrantReadWriteLock lifecycle = new ReentrantReadWriteLock(true);
        final Set<CursorState> cursors = ConcurrentHashMap.newKeySet();
        long database;

        State(long database) {
            this.database = database;
        }

        long requireOpen() throws Ohlc.Failure {
            if (database == 0) {
                throw new Ohlc.Failure(5, "Database is closed");
            }
            return database;
        }

        int closeNative() {
            lifecycle.writeLock().lock();
            try {
                if (database == 0) {
                    return 0;
                }
                for (CursorState cursor : cursors) {
                    Native.closeCursor(cursor.cursor);
                    cursor.cursor = 0;
                }
                cursors.clear();
                int status = Native.close(database);
                if (status == 0) {
                    database = 0;
                }
                return status;
            } finally {
                lifecycle.writeLock().unlock();
            }
        }

        @Override public void run() {
            closeNative();
        }
    }

    private static final class CursorState implements Runnable {
        final State owner;
        long cursor;

        CursorState(State owner, long cursor) {
            this.owner = owner;
            this.cursor = cursor;
        }

        @Override public void run() {
            owner.lifecycle.readLock().lock();
            try {
                synchronized (this) {
                    if (cursor != 0 && owner.cursors.remove(this)) {
                        Native.closeCursor(cursor);
                    }
                    cursor = 0;
                }
            } finally {
                owner.lifecycle.readLock().unlock();
            }
        }
    }

    private Database(long handle) {
        State owned;
        try {
            owned = new State(handle);
            cleanable = CLEANER.register(this, owned);
        } catch (RuntimeException | Error error) {
            Native.close(handle);
            throw error;
        }
        state = owned;
    }

    public static Database open(Path path) throws IOException {
        return open(path, false, Options.defaults());
    }

    public static Database open(Path path, boolean create, Options options) throws IOException {
        return new Database(Native.open(text(path.toString()), options.values(), create));
    }

    public byte[] uuid() throws IOException {
        state.lifecycle.readLock().lock();
        try {
            return Native.uuid(state.requireOpen());
        } finally {
            Reference.reachabilityFence(Database.this);
            state.lifecycle.readLock().unlock();
        }
    }

    @Override public void close() throws IOException {
        int status = state.closeNative();
        if (status != 0) {
            throw new Ohlc.Failure(status, "Could not close embedded database");
        }
        cleanable.clean();
    }

    public void checkpoint() throws IOException {
        state.lifecycle.readLock().lock();
        try {
            Native.checkpoint(state.requireOpen());
        } finally {
            Reference.reachabilityFence(Database.this);
            state.lifecycle.readLock().unlock();
        }
    }

    public Stats stats() throws IOException {
        state.lifecycle.readLock().lock();
        try {
            long[] v = Native.stats(state.requireOpen());
            return new Stats(v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7], v[8], v[9], v[10]);
        } finally {
            Reference.reachabilityFence(Database.this);
            state.lifecycle.readLock().unlock();
        }
    }

    public Table table(String name) throws IOException {
        state.lifecycle.readLock().lock();
        try {
            return decodeTable(Native.table(state.requireOpen(), text(name), 0));
        } finally {
            Reference.reachabilityFence(Database.this);
            state.lifecycle.readLock().unlock();
        }
    }

    public Table create(String name, long periodCount, boolean days, String timezone,
                        String description) throws IOException {
        Ohlc.uint32(periodCount);
        state.lifecycle.readLock().lock();
        try {
            return decodeTable(Native.create(state.requireOpen(), text(name), periodCount, days,
                                              text(timezone), text(description)));
        } finally {
            Reference.reachabilityFence(Database.this);
            state.lifecycle.readLock().unlock();
        }
    }

    /** Return at most limit tables. Continue at last.id + 1; maximum page is 256. */
    public List<Table> tables(long start, int limit) throws IOException {
        page(start, limit);
        start = Math.max(1, start);
        state.lifecycle.readLock().lock();
        try {
            long handle = state.requireOpen();
            long end = Math.min(stats().tableCount() + 1, start + limit);
            List<Table> result = new ArrayList<>();
            for (long id = start; id < end; id++) {
                result.add(decodeTable(Native.table(handle, null, id)));
            }
            return List.copyOf(result);
        } finally {
            Reference.reachabilityFence(Database.this);
            state.lifecycle.readLock().unlock();
        }
    }

    public Ohlc.Registration register(byte[] ticker) throws IOException {
        ticker(ticker);
        state.lifecycle.readLock().lock();
        try {
            long[] result = Native.ticker(state.requireOpen(), ticker, true);
            return new Ohlc.Registration(result[0], result[1]);
        } finally {
            Reference.reachabilityFence(Database.this);
            state.lifecycle.readLock().unlock();
        }
    }

    public Ohlc.Registration register(String ticker) throws IOException {
        return register(ticker.getBytes(StandardCharsets.UTF_8));
    }

    public long resolve(byte[] ticker) throws IOException {
        ticker(ticker);
        state.lifecycle.readLock().lock();
        try {
            return Native.ticker(state.requireOpen(), ticker, false)[0];
        } finally {
            Reference.reachabilityFence(Database.this);
            state.lifecycle.readLock().unlock();
        }
    }

    public long resolve(String ticker) throws IOException {
        return resolve(ticker.getBytes(StandardCharsets.UTF_8));
    }

    public List<Ohlc.Ticker> dictionary(long start, int limit) throws IOException {
        page(start, limit);
        state.lifecycle.readLock().lock();
        try {
            long handle = state.requireOpen();
            long end = Math.min(stats().tickerCount(), start + limit);
            List<Ohlc.Ticker> result = new ArrayList<>();
            for (long id = start; id < end; id++) {
                result.add(new Ohlc.Ticker(id, Native.tickerAt(handle, id)));
            }
            return List.copyOf(result);
        } finally {
            Reference.reachabilityFence(Database.this);
            state.lifecycle.readLock().unlock();
        }
    }

    private Table decodeTable(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        long id = Ohlc.u32(buffer);
        long sequence = buffer.getLong();
        boolean days = buffer.getInt() == 2;
        long period = Ohlc.u32(buffer);
        return new Table(id, sequence, days, period,
                         string(buffer), string(buffer), string(buffer));
    }

    public final class Table {
        public final long id;
        public final long createdSequence;
        public final boolean days;
        public final long periodCount;
        public final String name;
        public final String timezone;
        public final String description;

        private Table(long id, long sequence, boolean days, long period, String name,
                      String timezone, String description) {
            this.id = id;
            this.createdSequence = sequence;
            this.days = days;
            this.periodCount = period;
            this.name = name;
            this.timezone = timezone;
            this.description = description;
        }

        public long timeKey(String value) throws IOException {
            return Native.parse(days, text(timezone), text(value));
        }

        public String formatTime(long key) throws IOException {
            Ohlc.uint32(key);
            return Native.format(days, key);
        }

        /** Commit one batch without changing its position. A direct buffer is
         * borrowed for the call; a heap buffer is copied once. Do not mutate
         * the source until the call returns. No mutation is retried implicitly.
         */
        public long write(ByteBuffer rows) throws IOException {
            int size = rows.remaining();
            if (size == 0 || size % Ohlc.WRITE_BYTES != 0
                    || size / Ohlc.WRITE_BYTES > MAX_WRITE_ROWS) {
                throw new IllegalArgumentException("Expected 1..250000 complete 40-byte records");
            }
            ByteBuffer source = rows.duplicate();
            if (!source.isDirect()) {
                source = buffer(size).put(source).flip();
            }
            state.lifecycle.readLock().lock();
            try {
                return Native.write(state.requireOpen(), id, source, source.position(), size);
            } finally {
                Reference.reachabilityFence(this);
                Reference.reachabilityFence(Database.this);
                state.lifecycle.readLock().unlock();
            }
        }

        public long insert(String ticker, String time, int open, int high, int low, int close,
                           long volume, long amountBits, long factor) throws IOException {
            ByteBuffer row = buffer(Ohlc.WRITE_BYTES);
            Ohlc.putWrite(row, resolve(ticker), timeKey(time), open, high, low, close, volume,
                          amountBits, factor);
            return write(row.flip());
        }

        public Query series(String ticker, String start, String end) throws IOException {
            return series(resolve(ticker), timeKey(start),
                          end.equals("@4294967296") ? 0x100000000L : timeKey(end));
        }

        public Query series(long ticker, long start, long endExclusive) throws IOException {
            Ohlc.uint32(ticker);
            Ohlc.uint32(start);
            if (endExclusive < 0 || endExclusive > 0x100000000L) {
                throw new IllegalArgumentException("End must be in 0..2^32");
            }
            state.lifecycle.readLock().lock();
            try {
                return query(Native.series(state.requireOpen(), id, ticker, start, endExclusive));
            } finally {
                Reference.reachabilityFence(this);
                Reference.reachabilityFence(Database.this);
                state.lifecycle.readLock().unlock();
            }
        }

        public Query cross(String time) throws IOException {
            return cross(timeKey(time));
        }

        public Query cross(long time) throws IOException {
            Ohlc.uint32(time);
            state.lifecycle.readLock().lock();
            try {
                return query(Native.cross(state.requireOpen(), id, time));
            } finally {
                Reference.reachabilityFence(this);
                Reference.reachabilityFence(Database.this);
                state.lifecycle.readLock().unlock();
            }
        }
    }

    private Query query(long handle) {
        CursorState cursor = null;
        try {
            cursor = new CursorState(state, handle);
            state.cursors.add(cursor);
            return new Query(cursor, Native.sequence(handle));
        } catch (RuntimeException | Error error) {
            if (cursor != null) {
                state.cursors.remove(cursor);
            }
            Native.closeCursor(handle);
            throw error;
        }
    }

    /** Chunk data owns its direct buffer and remains valid after cursor close. */
    public record Chunk(ByteBuffer data, int count, long snapshotSequence) {}

    public final class Query implements AutoCloseable {
        private final CursorState cursor;
        private final Cleaner.Cleanable cleanup;
        private final long sequence;

        private Query(CursorState cursor, long sequence) {
            this.cursor = cursor;
            this.sequence = sequence;
            cleanup = CLEANER.register(this, cursor);
        }

        public long snapshotSequence() {
            return sequence;
        }

        /** Fill complete 36-byte results into a writable direct buffer and
         * advance its position. Return zero at EOF; at most 16384 rows per call.
         * Reusing the buffer avoids per-chunk allocation. Calls on this cursor
         * are serialized; different cursors may execute concurrently.
         */
        public int read(ByteBuffer target) throws IOException {
            if (!target.isDirect() || target.isReadOnly()
                    || target.remaining() < Ohlc.RESULT_BYTES) {
                throw new IllegalArgumentException("Expected a writable direct result buffer");
            }
            state.lifecycle.readLock().lock();
            try {
                synchronized (cursor) {
                    if (cursor.cursor == 0) {
                        return 0;
                    }
                    state.requireOpen();
                    int count;
                    try {
                        int capacity = Math.min(16384, target.remaining() / Ohlc.RESULT_BYTES);
                        count = Native.next(cursor.cursor, target, target.position(), capacity);
                    } catch (IOException error) {
                        cleanup.clean();
                        throw error;
                    }
                    target.position(target.position() + count * Ohlc.RESULT_BYTES);
                    if (count == 0) {
                        cleanup.clean();
                    }
                    return count;
                }
            } finally {
                Reference.reachabilityFence(this);
                Reference.reachabilityFence(Database.this);
                state.lifecycle.readLock().unlock();
            }
        }

        public Chunk next() throws IOException {
            ByteBuffer data = buffer(4096 * Ohlc.RESULT_BYTES);
            int count = read(data);
            if (count == 0) {
                return null;
            }
            data.flip();
            return new Chunk(data.asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN),
                             count, sequence);
        }

        @Override public void close() {
            cleanup.clean();
        }
    }

    public static ByteBuffer buffer(int bytes) {
        return ByteBuffer.allocateDirect(bytes).order(ByteOrder.LITTLE_ENDIAN);
    }

    private static byte[] text(String value) {
        if (value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("Text cannot contain NUL");
        }
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String string(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.getInt()];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static void ticker(byte[] value) {
        if (value.length < 1 || value.length > 4096) {
            throw new IllegalArgumentException("Ticker must contain 1..4096 bytes");
        }
    }

    private static void page(long start, int limit) {
        Ohlc.uint32(start);
        if (limit < 1 || limit > 256) {
            throw new IllegalArgumentException("Page limit must be 1..256");
        }
    }
}
