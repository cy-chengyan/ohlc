// SPDX-License-Identifier: Apache-2.0
package io.ohlc;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;

/** Java 17 client. A connection supports one request at a time, with no retries.
 * Batch buffers use explicit little endian order. All uint32 values are longs;
 * uint64 values preserve every bit in a long, including negative signed values.
 */
public final class Ohlc implements AutoCloseable {
    public static final String VERSION = "0.1.0-beta.1";
    public static final int ROW_BYTES = 32;
    public static final int WRITE_BYTES = 40;
    public static final int RESULT_BYTES = 36;
    private static final int FRAME_LIMIT = 16 * 1024 * 1024;
    private static final ScheduledThreadPoolExecutor DEADLINES = deadlines();
    private static final Pattern TIME = Pattern.compile(
        "^(?:([0-9]{4,8})-([0-9]{2})-([0-9]{2})|([0-9]{4})([0-9]{2})([0-9]{2}))"
        + "(?:[ T]([0-9]{2}):([0-9]{2}):00(Z|[+-][0-9]{2}:[0-9]{2})?)?$");

    private final Closeable transport;
    private final DataInputStream input;
    private final OutputStream output;
    private final int timeoutMs;
    private byte[] uuid;
    private int maxFrame = FRAME_LIMIT;
    private int maxRows;
    private long queryMs;
    private int capabilities;
    private long request;
    private int opcode;
    private long chunk;
    private Query active;
    private volatile boolean closed;
    private ScheduledFuture<?> alarm;
    private final Object deadlineLock = new Object();
    private long deadlineGeneration;

    public static final class Failure extends IOException {
        private static final long serialVersionUID = 1L;
        public final int status;

        public Failure(int status, String message) {
            super(message);
            this.status = status;
        }
    }

    private static ScheduledThreadPoolExecutor deadlines() {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, task -> {
            Thread thread = new Thread(task, "ohlc-deadline");
            thread.setDaemon(true);
            return thread;
        });
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

    private Ohlc(Closeable transport, InputStream input, OutputStream output, int timeoutMs) {
        if (timeoutMs <= 0) {
            throw new IllegalArgumentException("timeoutMs must be positive");
        }
        this.transport = transport;
        this.input = new DataInputStream(input);
        this.output = output;
        this.timeoutMs = timeoutMs;
    }

    /** A null TLS context selects unencrypted TCP. An empty token selects
     * anonymous access when the server has no credentials configured.
     * TLS verifies the certificate chain and endpoint identity.
     */
    public static Ohlc connect(String host, int port, SSLContext tls, byte[] token, int timeoutMs)
            throws IOException {
        if (timeoutMs <= 0) {
            throw new IllegalArgumentException("timeoutMs must be positive");
        }
        Socket socket = new Socket();
        try {
            InetAddress address = InetAddress.getByName(host);
            socket.connect(new InetSocketAddress(address, port), timeoutMs);
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(timeoutMs);
            if (tls != null) {
                socket = tls.getSocketFactory().createSocket(socket, host, port, true);
                SSLParameters parameters = ((SSLSocket) socket).getSSLParameters();
                parameters.setEndpointIdentificationAlgorithm("HTTPS");
                ((SSLSocket) socket).setSSLParameters(parameters);
            }
            Ohlc client = new Ohlc(socket, socket.getInputStream(), socket.getOutputStream(), timeoutMs);
            client.arm(timeoutMs);
            try {
                if (socket instanceof SSLSocket secure) {
                    secure.startHandshake();
                }
                client.hello(token);
            } finally {
                client.disarm();
            }
            return client;
        } catch (IOException | RuntimeException error) {
            socket.close();
            throw error;
        }
    }

    public static Ohlc unix(Path path, byte[] token, int timeoutMs) throws IOException {
        if (timeoutMs <= 0) {
            throw new IllegalArgumentException("timeoutMs must be positive");
        }
        SocketChannel channel = SocketChannel.open(StandardProtocolFamily.UNIX);
        Ohlc client = new Ohlc(channel, Channels.newInputStream(channel),
                              Channels.newOutputStream(channel), timeoutMs);
        try {
            client.arm(timeoutMs);
            channel.connect(UnixDomainSocketAddress.of(path));
            client.hello(token);
            return client;
        } catch (IOException | RuntimeException error) {
            client.close();
            throw error;
        } finally {
            client.disarm();
        }
    }

    private void hello(byte[] token) throws IOException {
        if (token.length > 4096) {
            throw new IllegalArgumentException("Token exceeds 4096 bytes");
        }
        ByteBuffer response = call(1, string(token));
        exact(response, 32);
        uuid = new byte[16];
        response.get(uuid);
        long frame = u32(response);
        long rows = u32(response);
        queryMs = u32(response);
        capabilities = response.getInt();
        if (frame < 32 || frame > FRAME_LIMIT || rows == 0 || rows > 250000 ||
                queryMs == 0 || capabilities < 1 || capabilities > 3) {
            throw corrupt("Invalid HELLO capabilities");
        }
        maxFrame = (int) frame;
        maxRows = (int) rows;
    }

    public byte[] uuid() {
        return uuid.clone();
    }

    public int maxWriteRows() {
        return maxRows;
    }

    public int capabilities() {
        return capabilities;
    }

    private void abort() {
        closed = true;
        try {
            transport.close();
        } catch (IOException ignored) {
            // The original request failure remains the authoritative error.
        }
    }

    private void arm(long milliseconds) {
        synchronized (deadlineLock) {
            disarm();
            long generation = deadlineGeneration;
            alarm = DEADLINES.schedule(() -> {
                synchronized (deadlineLock) {
                    if (generation == deadlineGeneration) {
                        abort();
                    }
                }
            }, milliseconds, TimeUnit.MILLISECONDS);
        }
    }

    private void disarm() {
        synchronized (deadlineLock) {
            deadlineGeneration++;
            if (alarm != null) {
                alarm.cancel(false);
                alarm = null;
            }
        }
    }

    @Override
    public synchronized void close() {
        disarm();
        abort();
    }

    private Failure corrupt(String message) {
        abort();
        return new Failure(6, message);
    }

    private void begin(int operation, ByteBuffer body, long deadlineMs) throws IOException {
        if (closed) {
            throw new Failure(5, "Connection is closed");
        }
        if (active != null) {
            throw new Failure(4, "Consume or close the active query first");
        }
        if (body.remaining() > maxFrame || request == -1L) {
            throw new Failure(3, "Request limit exceeded");
        }
        request++;
        opcode = operation;
        chunk = 0;
        arm(deadlineMs);
        ByteBuffer header = buffer(32);
        header.put(new byte[] {'O', 'H', 'L', 'C'}).putShort((short) 3).putShort((short) opcode)
              .putInt(0).putInt(body.remaining()).putLong(request).putInt(0).putInt(0);
        output.write(header.array());
        ByteBuffer source = body.duplicate();
        if (source.hasArray()) {
            output.write(source.array(), source.arrayOffset() + source.position(), source.remaining());
        } else {
            byte[] part = new byte[Math.min(source.remaining(), 65536)];
            while (source.hasRemaining()) {
                int length = Math.min(source.remaining(), part.length);
                source.get(part, 0, length);
                output.write(part, 0, length);
            }
        }
        output.flush();
    }

    private record Frame(ByteBuffer body, boolean last, int status) {}

    private Frame receive() throws IOException {
        byte[] header = new byte[32];
        input.readFully(header);
        ByteBuffer fields = ByteBuffer.wrap(header).order(ByteOrder.LITTLE_ENDIAN);
        if (fields.getInt() != 0x434c484f || fields.getShort() != 3 ||
                Short.toUnsignedInt(fields.getShort()) != opcode) {
            throw corrupt("Invalid response header");
        }
        int flags = fields.getInt();
        long size = u32(fields);
        long id = fields.getLong();
        int status = fields.getInt();
        long index = u32(fields);
        if ((flags != 1 && flags != 3) || size > maxFrame || id != request || index != chunk ||
                status < 0 || status > 11 || (status != 0 && (flags != 3 || size != 0)) ||
                (index == 0xffffffffL && flags != 3)) {
            throw corrupt("Response identity, size or sequence mismatch");
        }
        byte[] body = new byte[(int) size];
        input.readFully(body);
        chunk++;
        return new Frame(ByteBuffer.wrap(body).order(ByteOrder.LITTLE_ENDIAN), flags == 3, status);
    }

    private synchronized ByteBuffer call(int operation, ByteBuffer body) throws IOException {
        boolean mutation = operation == 7 || operation == 8 || operation == 9 || operation == 13;
        long previous = request;
        Frame frame;
        try {
            begin(operation, body, timeoutMs);
            frame = receive();
            if (!frame.last) {
                throw corrupt("Metadata operation must finish in one frame");
            }
        } catch (IOException error) {
            if (request != previous) {
                abort();
                if (mutation) {
                    throw new Failure(9, "Mutation outcome is unknown: " + error.getMessage());
                }
            }
            throw error;
        } finally {
            if (active == null) {
                disarm();
            }
        }
        if (frame.status != 0) {
            throw new Failure(frame.status, "Server status " + frame.status);
        }
        return frame.body;
    }

    private void exact(ByteBuffer body, int expected) throws Failure {
        if (body.remaining() != expected) {
            throw corrupt("Unexpected response length");
        }
    }

    public synchronized void ping() throws IOException {
        exact(call(2, buffer(0)), 0);
    }

    /** I/O counters are cumulative since open, not live disk usage. */
    public record Stats(long commitSequence, long checkpointSequence, long tickerCount,
                        long tableCount, long memoryBytes, long diskReadBytes,
                        long diskReadCalls, long cacheHits, long cacheMisses,
                        long walBytes, long dataBytes) {}

    public synchronized Stats stats() throws IOException {
        ByteBuffer body = call(12, buffer(0));
        exact(body, 88);
        long commit = body.getLong();
        long checkpoint = body.getLong();
        long tickers = body.getLong();
        long tables = body.getLong();
        if (tables < 0 || tables > 0xffffffffL) {
            throw corrupt("Invalid STATS table count");
        }
        return new Stats(commit, checkpoint, tickers, tables, body.getLong(), body.getLong(),
                         body.getLong(), body.getLong(), body.getLong(), body.getLong(),
                         body.getLong());
    }

    /** Requires write access. A disconnect may leave the outcome unknown. */
    public synchronized void checkpoint() throws IOException {
        if (call(13, buffer(0)).hasRemaining()) {
            abort();
            throw new Failure(9, "Malformed checkpoint acknowledgement");
        }
    }

    public synchronized long resolve(byte[] ticker) throws IOException {
        ticker(ticker);
        ByteBuffer response = call(3, string(ticker));
        exact(response, 4);
        return u32(response);
    }

    public long resolve(String ticker) throws IOException {
        return resolve(ticker.getBytes(StandardCharsets.UTF_8));
    }

    public record Registration(long code, long commitSequence) {}

    public synchronized Registration register(byte[] ticker) throws IOException {
        ticker(ticker);
        ByteBuffer response = call(7, string(ticker));
        if (response.remaining() != 12) {
            abort();
            throw new Failure(9, "Malformed registration acknowledgement");
        }
        return new Registration(u32(response), response.getLong());
    }

    public Registration register(String ticker) throws IOException {
        return register(ticker.getBytes(StandardCharsets.UTF_8));
    }

    public synchronized Table table(String name) throws IOException {
        ByteBuffer response = call(10, string(text(name)));
        Table table = decodeTable(response);
        exact(response, 0);
        return table;
    }

    public synchronized Table create(String name, long periodCount, boolean days,
                                      String timezone, String description) throws IOException {
        byte[] encodedName = text(name);
        byte[] encodedZone = text(timezone);
        byte[] encodedDescription = text(description);
        if (encodedName.length > 63 || encodedZone.length > 255 || encodedDescription.length > 4096 ||
                periodCount == 0) {
            throw new IllegalArgumentException("Table definition exceeds limits");
        }
        ByteBuffer body = buffer(20 + encodedName.length + encodedZone.length + encodedDescription.length);
        body.put(string(encodedName)).putInt(days ? 2 : 1).putInt(uint32(periodCount))
            .put(string(encodedZone)).put(string(encodedDescription)).flip();
        ByteBuffer response = call(9, body);
        if (response.remaining() != 12) {
            abort();
            throw new Failure(9, "Malformed table creation acknowledgement");
        }
        return new Table(u32(response), response.getLong(), name, days, periodCount, timezone, description);
    }

    /** Fetch a bounded table page. Continue at last.id + 1; never wrap uint32. */
    public synchronized List<Table> tables(long start, int limit) throws IOException {
        pageLimit(limit);
        ByteBuffer response = call(11, buffer(8).putInt(uint32(start)).putInt(limit).flip());
        if (response.remaining() < 12) {
            throw corrupt("Truncated table page");
        }
        response.getLong();
        long count = u32(response);
        if (count > limit) {
            throw corrupt("Oversized table page");
        }
        List<Table> tables = new ArrayList<>();
        for (long i = 0; i < count; i++) {
            Table table = decodeTable(response);
            if (table.id < start) {
                throw corrupt("Unordered table page");
            }
            start = table.id + 1;
            tables.add(table);
        }
        exact(response, 0);
        return List.copyOf(tables);
    }

    public record Ticker(long code, byte[] bytes) {
        public Ticker {
            bytes = bytes.clone();
        }
        @Override public byte[] bytes() {
            return bytes.clone();
        }
    }

    public synchronized List<Ticker> dictionary(long start, int limit) throws IOException {
        pageLimit(limit);
        ByteBuffer response = call(4, buffer(8).putInt(uint32(start)).putInt(limit).flip());
        if (response.remaining() < 12) {
            throw corrupt("Truncated dictionary page");
        }
        response.getLong();
        long count = u32(response);
        if (count > limit) {
            throw corrupt("Oversized dictionary page");
        }
        List<Ticker> entries = new ArrayList<>();
        for (long i = 0; i < count; i++) {
            if (response.remaining() < 4) {
                throw corrupt("Truncated ticker code");
            }
            long code = u32(response);
            byte[] bytes = readString(response, 4096);
            if (code < start || bytes.length == 0) {
                throw corrupt("Invalid dictionary ordering");
            }
            start = code + 1;
            entries.add(new Ticker(code, bytes));
        }
        exact(response, 0);
        return List.copyOf(entries);
    }

    private static void pageLimit(int limit) {
        if (limit < 1 || limit > 256) {
            throw new IllegalArgumentException("Page size must be 1..256");
        }
    }

    private Table decodeTable(ByteBuffer body) throws IOException {
        if (body.remaining() < 12) {
            throw corrupt("Truncated table definition");
        }
        long id = u32(body);
        long sequence = body.getLong();
        String name = utf8(readString(body, 63));
        if (body.remaining() < 8) {
            throw corrupt("Truncated period definition");
        }
        long unit = u32(body);
        long count = u32(body);
        String zone = utf8(readString(body, 255));
        String description = utf8(readString(body, 4096));
        if (id == 0 || sequence == 0 || (unit != 1 && unit != 2) || count == 0) {
            throw corrupt("Invalid table definition");
        }
        return new Table(id, sequence, name, unit == 2, count, zone, description);
    }

    private byte[] readString(ByteBuffer body, int maximum) throws IOException {
        if (body.remaining() < 4) {
            throw corrupt("Truncated string length");
        }
        long size = u32(body);
        if (size > maximum || size > body.remaining()) {
            throw corrupt("Invalid string length");
        }
        byte[] value = new byte[(int) size];
        body.get(value);
        return value;
    }

    private String utf8(byte[] bytes) throws IOException {
        String text = new String(bytes, StandardCharsets.UTF_8);
        if (text.indexOf('\0') >= 0 || !Arrays.equals(bytes, text.getBytes(StandardCharsets.UTF_8))) {
            throw corrupt("Invalid UTF-8 metadata");
        }
        return text;
    }

    public final class Table {
        public final long id;
        public final long createdSequence;
        public final String name;
        public final boolean days;
        public final long periodCount;
        public final String timezone;
        public final String description;

        private Table(long id, long sequence, String name, boolean days, long period,
                      String timezone, String description) {
            this.id = id;
            this.createdSequence = sequence;
            this.name = name;
            this.days = days;
            this.periodCount = period;
            this.timezone = timezone;
            this.description = description;
        }

        public long timeKey(String text) {
            if (text.matches("@[0-9]+")) {
                return Integer.toUnsignedLong(uint32(Long.parseLong(text.substring(1))));
            }
            Matcher match = TIME.matcher(text);
            if (!match.matches() || days != (match.group(7) == null)) {
                throw new IllegalArgumentException("Invalid date or minute; seconds must be 00");
            }
            int base = match.group(1) != null ? 1 : 4;
            LocalDate date = LocalDate.of(Integer.parseInt(match.group(base)),
                Integer.parseInt(match.group(base + 1)), Integer.parseInt(match.group(base + 2)));
            if (date.getYear() < 1) {
                throw new IllegalArgumentException("Year must be positive");
            }
            if (days) {
                return Integer.toUnsignedLong(uint32(date.toEpochDay()));
            }
            LocalDateTime local = date.atTime(Integer.parseInt(match.group(7)),
                                               Integer.parseInt(match.group(8)));
            String suffix = match.group(9);
            long epoch;
            if (suffix != null) {
                int offset = 0;
                if (!suffix.equals("Z")) {
                    int hours = Integer.parseInt(suffix.substring(1, 3));
                    int minutes = Integer.parseInt(suffix.substring(4, 6));
                    if (hours > 23 || minutes > 59) {
                        throw new IllegalArgumentException("Invalid UTC offset");
                    }
                    offset = (hours * 3600 + minutes * 60) * (suffix.charAt(0) == '+' ? 1 : -1);
                }
                epoch = local.toEpochSecond(ZoneOffset.UTC) - offset;
            } else {
                List<ZoneOffset> offsets = ZoneId.of(timezone).getRules().getValidOffsets(local);
                if (offsets.size() != 1) {
                    throw new DateTimeException("Ambiguous or nonexistent local time; specify an offset");
                }
                epoch = local.toEpochSecond(offsets.get(0));
            }
            if (epoch < 0 || epoch % 60 != 0) {
                throw new IllegalArgumentException("Minute is outside the time-key domain");
            }
            return Integer.toUnsignedLong(uint32(epoch / 60));
        }

        public String formatTime(long key) {
            uint32(key);
            if (days) {
                LocalDate date = LocalDate.ofEpochDay(key);
                return String.format(Locale.ROOT, "%04d-%02d-%02d", date.getYear(),
                                     date.getMonthValue(), date.getDayOfMonth());
            }
            LocalDateTime time = LocalDateTime.ofInstant(Instant.ofEpochSecond(key * 60), ZoneOffset.UTC);
            return String.format(Locale.ROOT, "%04d-%02d-%02dT%02d:%02d:00Z", time.getYear(),
                                 time.getMonthValue(), time.getDayOfMonth(), time.getHour(), time.getMinute());
        }

        /** Commit one encoded batch; the supplied buffer position is unchanged. */
        public long write(ByteBuffer rows) throws IOException {
            if (rows.remaining() == 0 || rows.remaining() % WRITE_BYTES != 0 ||
                    rows.remaining() / WRITE_BYTES > maxRows) {
                throw new IllegalArgumentException("Invalid encoded batch length");
            }
            ByteBuffer body = buffer(8 + rows.remaining());
            body.putInt(uint32(id)).putInt(rows.remaining() / WRITE_BYTES).put(rows.duplicate()).flip();
            synchronized (Ohlc.this) {
                ByteBuffer response = call(8, body);
                if (response.remaining() != 8) {
                    abort();
                    throw new Failure(9, "Malformed write acknowledgement");
                }
                return response.getLong();
            }
        }

        public long insert(String ticker, String time, int open, int high, int low, int close,
                           long volume, long amountBits, long factor) throws IOException {
            ByteBuffer row = buffer(WRITE_BYTES);
            putWrite(row, resolve(ticker), timeKey(time), open, high, low, close, volume, amountBits, factor);
            return write(row.flip());
        }

        public Query series(String ticker, String start, String end) throws IOException {
            return series(resolve(ticker), timeKey(start),
                          end.equals("@4294967296") ? 0x100000000L : timeKey(end));
        }

        public Query series(long ticker, long start, long endExclusive) throws IOException {
            if (endExclusive < start || endExclusive > 0x100000000L) {
                throw new IllegalArgumentException("Invalid half-open range");
            }
            ByteBuffer body = buffer(20).putInt(uint32(id)).putInt(uint32(ticker))
                .putInt(uint32(start)).putLong(endExclusive).flip();
            return query(5, body, id, start, endExclusive);
        }

        public Query cross(String time) throws IOException {
            return cross(timeKey(time));
        }

        public Query cross(long key) throws IOException {
            return query(6, buffer(8).putInt(uint32(id)).putInt(uint32(key)).flip(),
                         id, 0, 0x100000000L);
        }
    }

    private synchronized Query query(int operation, ByteBuffer body, long table, long start, long end)
            throws IOException {
        long previous = request;
        try {
            begin(operation, body, Math.min(timeoutMs, queryMs));
            active = new Query(table, start, end);
            return active;
        } catch (IOException error) {
            if (request != previous) {
                disarm();
                abort();
            }
            throw error;
        }
    }

    public record Chunk(ByteBuffer data, int count, long snapshotSequence, boolean last) {}

    /** Close an unfinished query to cancel it and close its connection. */
    public final class Query implements AutoCloseable {
        private final long table;
        private final long start;
        private final long end;
        private long emitted;
        private long snapshot;
        private long lastKey;
        private boolean complete;
        private boolean first = true;

        private Query(long table, long start, long end) {
            this.table = table;
            this.start = start;
            this.end = end;
        }

        /** Returns null after successful FINAL. Each chunk owns its backing array. */
        public Chunk next() throws IOException {
            synchronized (Ohlc.this) {
                if (complete) {
                    return null;
                }
                if (active != this || closed) {
                    throw new Failure(5, "Query connection is closed");
                }
                try {
                    Frame frame = receive();
                    if (frame.status != 0) {
                        throw new Failure(frame.status, "Query failed after " + emitted + " rows");
                    }
                    ByteBuffer body = frame.body;
                    if (body.remaining() < 32) {
                        throw corrupt("Truncated query metadata");
                    }
                    long sequence = body.getLong();
                    long total = body.getLong();
                    long count = u32(body);
                    long kind = u32(body);
                    long tableId = u32(body);
                    int reserved = body.getInt();
                    if (count * RESULT_BYTES != body.remaining() || (!frame.last && count == 0) ||
                            kind != opcode - 4 || tableId != table || reserved != 0 ||
                            emitted > Long.MAX_VALUE - count || total != emitted + count ||
                            (!first && sequence != snapshot)) {
                        throw corrupt("Invalid query metadata or cumulative count");
                    }
                    for (int i = body.position(); i < body.limit(); i += RESULT_BYTES) {
                        long key = Integer.toUnsignedLong(body.getInt(i));
                        if ((emitted != 0 || i != body.position()) && key <= lastKey ||
                                key < start || key >= end) {
                            throw corrupt("Unordered or out-of-range query key");
                        }
                        lastKey = key;
                    }
                    first = false;
                    snapshot = sequence;
                    emitted = total;
                    complete = frame.last;
                    if (complete) {
                        active = null;
                        disarm();
                    }
                    ByteBuffer rows = body.slice().asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN);
                    return new Chunk(rows, (int) count, snapshot, complete);
                } catch (IOException error) {
                    active = null;
                    disarm();
                    abort();
                    throw error;
                }
            }
        }

        @Override
        public void close() {
            synchronized (Ohlc.this) {
                if (!complete) {
                    complete = true;
                    active = null;
                    Ohlc.this.close();
                }
            }
        }
    }

    public static ByteBuffer buffer(int bytes) {
        return ByteBuffer.allocate(bytes).order(ByteOrder.LITTLE_ENDIAN);
    }

    public static int uint32(long value) {
        if (value < 0 || value > 0xffffffffL) {
            throw new IllegalArgumentException("Value is outside uint32: " + value);
        }
        return (int) value;
    }

    public static long u32(ByteBuffer buffer) {
        return Integer.toUnsignedLong(buffer.getInt());
    }

    /** amountBits accepts every long bit pattern; use Long.parseUnsignedLong
     * and Long.toUnsignedString when converting uint64 amount to/from text.
     */
    public static void putWrite(ByteBuffer buffer, long ticker, long time, int open, int high,
                                int low, int close, long volume, long amountBits, long factor) {
        buffer.order(ByteOrder.LITTLE_ENDIAN).putInt(uint32(ticker)).putInt(uint32(time))
              .putInt(open).putInt(high).putInt(low).putInt(close).putInt(uint32(volume))
              .putLong(amountBits).putInt(uint32(factor));
    }

    private static ByteBuffer string(byte[] bytes) {
        return buffer(4 + bytes.length).putInt(bytes.length).put(bytes).flip();
    }

    private static byte[] text(String value) {
        if (value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("Metadata cannot contain NUL");
        }
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static void ticker(byte[] value) {
        if (value.length < 1 || value.length > 4096) {
            throw new IllegalArgumentException("Ticker length must be 1..4096 bytes");
        }
    }
}
