// SPDX-License-Identifier: Apache-2.0
import io.ohlc.Database;
import io.ohlc.Ohlc;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public final class JavaEmbeddedTest {
    private JavaEmbeddedTest() {}

    private static void checkRow(ByteBuffer row, long key) {
        assert Ohlc.u32(row) == key;
        assert row.getInt() == Integer.MIN_VALUE;
        assert row.getInt() == Integer.MAX_VALUE;
        assert row.getInt() == -1;
        assert row.getInt() == 0;
        assert Ohlc.u32(row) == 0xffffffffL;
        assert Long.toUnsignedString(row.getLong()).equals("18446744073709551615");
        assert Ohlc.u32(row) == 0xffffffffL;
    }

    private static void put(ByteBuffer rows, long ticker, long key) {
        Ohlc.putWrite(rows, ticker, key, Integer.MIN_VALUE, Integer.MAX_VALUE, -1, 0,
                      0xffffffffL, -1L, 0xffffffffL);
    }

    private static void closeRace(Database db, Database.Table table, long key) throws Exception {
        CyclicBarrier gate = new CyclicBarrier(3);
        ExecutorService threads = Executors.newFixedThreadPool(2);
        try {
            Future<?> reader = threads.submit(() -> {
                gate.await(5, TimeUnit.SECONDS);
                try {
                    for (int i = 0; i < 32; i++) {
                        try (Database.Query query = table.cross(key)) {
                            Database.Chunk chunk = query.next();
                            if (chunk != null) {
                                assert chunk.count() == 2;
                            }
                        }
                    }
                } catch (Ohlc.Failure error) {
                    assert error.status == 5;
                }
                return null;
            });
            Future<?> writer = threads.submit(() -> {
                gate.await(5, TimeUnit.SECONDS);
                try {
                    for (int i = 0; i < 16; i++) {
                        table.insert("AAPL", "@" + (key + 20000 + i), 1, 1, 1, 1, 1, 1, 1);
                    }
                } catch (Ohlc.Failure error) {
                    assert error.status == 5;
                }
                return null;
            });
            gate.await(5, TimeUnit.SECONDS);
            db.close();
            reader.get(10, TimeUnit.SECONDS);
            writer.get(10, TimeUnit.SECONDS);
        } finally {
            threads.shutdownNow();
        }
    }

    public static void main(String[] arguments) throws Exception {
        Path path = Path.of(arguments[0]);
        Database.Query outstanding;
        Database.Chunk retained;
        byte[] uuid;
        try (Database db = Database.open(path)) {
            uuid = db.uuid();
            try (Database second = Database.open(path)) {
                throw new AssertionError("Second directory owner accepted: " + second);
            } catch (Ohlc.Failure error) {
                assert error.status == 4;
            }
            Database.Table minute = db.table("bars_3m");
            assert minute.description.equals("UTF-8 \ud83d\udcc8");
            assert db.tables(1, 128).size() == 2;
            Database.Table removed = db.create("java_drop", 1, true, "", "");
            db.drop("java_drop");
            Database.Table replacement = db.create("java_drop", 1, true, "", "");
            assert replacement.id > removed.id;
            assert db.tables(removed.id, 1).get(0).id == replacement.id;
            db.drop("java_drop");
            assert db.dictionary(0, 128).size() == 2;
            byte[] binary = {'0', '0', 0, (byte) 255, '\n'};
            assert db.resolve(binary) == 1;
            assert Arrays.equals(db.dictionary(1, 1).get(0).bytes(), binary);
            long key = minute.timeKey("20260901 09:30:00");
            assert key == minute.timeKey("2026-09-01T01:30:00Z");
            assert minute.timeKey(minute.formatTime(0xffffffffL)) == 0xffffffffL;
            long count = 0;
            ByteBuffer output = Database.buffer(19 * Ohlc.RESULT_BYTES + 7);
            try (Database.Query query = minute.series(db.resolve("AAPL"), key, key + 1800)) {
                while (true) {
                    output.clear().position(3).limit(output.capacity() - 4);
                    int rows = query.read(output);
                    assert output.position() == 3 + rows * Ohlc.RESULT_BYTES;
                    if (rows == 0) {
                        break;
                    }
                    output.flip().position(3);
                    while (output.hasRemaining()) {
                        checkRow(output, key + count++ * 3);
                    }
                }
            }
            assert count == 600;
            try (Database.Query query = minute.cross(key)) {
                retained = query.next();
                assert retained.count() == 2 && retained.data().isReadOnly();
            }
            try (Database.Query query = minute.cross(key - 1)) {
                assert query.next() == null;
            }
            Database.Table day = db.create("java_day", 5, true, "", "JNI \ud83d\ude80");
            long ticker = db.register("JAVA").code();
            long date = day.timeKey("20260901");
            assert day.timeKey(day.formatTime(0xffffffffL)) == 0xffffffffL;
            // Heap and sliced direct inputs preserve position and uint64 bits.
            ByteBuffer heap = ByteBuffer.allocate(43).order(ByteOrder.LITTLE_ENDIAN);
            heap.position(3);
            put(heap, ticker, date);
            heap.flip().position(3);
            day.write(heap.asReadOnlyBuffer());
            assert heap.position() == 3;
            ByteBuffer direct = Database.buffer(128);
            direct.position(4);
            ByteBuffer slice = direct.slice().order(ByteOrder.LITTLE_ENDIAN);
            put(slice, ticker, date + 1);
            put(slice, ticker, date + 2);
            slice.flip();
            day.write(slice);
            assert slice.position() == 0;
            try (Database.Query snapshot = day.cross(date)) {
                day.insert("JAVA", "20260901", 1, 1, 1, 1, 1, 1, 1);
                checkRow(snapshot.next().data(), ticker);
            }
            day.write(heap);
            long sequence = db.stats().commitSequence();
            try {
                ByteBuffer duplicate = Database.buffer(80);
                put(duplicate, ticker, date);
                put(duplicate, ticker, date);
                day.write(duplicate.flip());
                throw new AssertionError("Duplicate batch accepted");
            } catch (Ohlc.Failure error) {
                assert error.status == 1;
            }
            assert db.stats().commitSequence() == sequence;
            db.checkpoint();
            assert db.stats().checkpointSequence() == sequence;
            heap.clear();
            put(heap, ticker, date + 3);
            day.write(heap.flip());
            outstanding = minute.cross(key);
            closeRace(db, minute, key);
        }
        assert outstanding.next() == null;
        outstanding.close();
        checkRow(retained.data(), 0);
        try (Database reopened = Database.open(path)) {
            assert Arrays.equals(uuid, reopened.uuid());
            try (Database.Query query = reopened.table("java_day").cross("20260904")) {
                checkRow(query.next().data(), reopened.resolve("JAVA"));
            }
        }
        System.out.println("Java JNI, UTF-8, direct buffers, snapshots, close races and recovery: OK");
    }
}
