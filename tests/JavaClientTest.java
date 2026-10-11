// SPDX-License-Identifier: Apache-2.0
import io.ohlc.Ohlc;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;

public final class JavaClientTest {
    private JavaClientTest() {}

    private static void nullableRows(Ohlc client) throws Exception {
        Ohlc.Table table = client.create("java_nullable", 1, true, "", "");
        for (int mask = 0; mask < 256; mask++) {
            Ohlc.Row value = new Ohlc.Row((mask & 1) == 0 ? null : Long.MIN_VALUE,
                    (mask & 2) == 0 ? null : Long.MAX_VALUE,
                    (mask & 4) == 0 ? null : -1L, (mask & 8) == 0 ? null : 0L,
                    (mask & 16) == 0 ? null : -1L, (mask & 32) == 0 ? null : -1L,
                    (mask & 64) == 0 ? null : -1L,
                    (mask & 128) == 0 ? null : Integer.MIN_VALUE);
            table.insert("NULLABLE", "@" + mask, value);
            try (Ohlc.Query query = table.cross(mask)) {
                Ohlc.Chunk chunk = query.next();
                assert chunk.count() == 1;
                ByteBuffer data = chunk.data();
                assert Ohlc.u32(data) == 0;
                assert Ohlc.getRow(data).equals(value);
                assert !data.hasRemaining();
                Ohlc.Chunk tail;
                while ((tail = query.next()) != null) {
                    assert tail.count() == 0 && tail.last();
                }
            }
        }
        table.insert("NULLABLE", "@256", 0, 0, 0, 0, 0, 0, 0, 0);
        try (Ohlc.Query query = table.cross(256)) {
            ByteBuffer data = query.next().data();
            Ohlc.u32(data);
            assert Ohlc.getRow(data).reserved().equals(0);
            Ohlc.Chunk tail;
            while ((tail = query.next()) != null) {
                assert tail.count() == 0 && tail.last();
            }
        }
        table.insert("NULLABLE", "@256", 0, 0, 0, 0, 0, 0, 0);
        try (Ohlc.Query query = table.cross(256)) {
            ByteBuffer data = query.next().data();
            Ohlc.u32(data);
            assert Ohlc.getRow(data).reserved() == null;
            Ohlc.Chunk tail;
            while ((tail = query.next()) != null) {
                assert tail.count() == 0 && tail.last();
            }
        }
        client.drop("java_nullable");
    }

    private static void extendedPeriods(Ohlc client) throws Exception {
        Ohlc.PeriodUnit[] units = {Ohlc.PeriodUnit.SECOND, Ohlc.PeriodUnit.MONTH,
                                   Ohlc.PeriodUnit.YEAR};
        for (Ohlc.PeriodUnit unit : units) {
            boolean seconds = unit == Ohlc.PeriodUnit.SECOND;
            Ohlc.Table table = client.create("java_period", 5, unit,
                                              seconds ? "Asia/Shanghai" : "", "");
            assert client.table("java_period").periodUnit == unit;
            String stamp = seconds ? "20260901 09:30:17" : "20260917";
            long key = seconds ? 1788226217L : 20713L;
            assert table.timeKey(stamp) == key;
            assert table.timeKey(table.formatTime(0xffffffffL)) == 0xffffffffL;
            assert table.formatTime(key).equals(seconds ? "2026-09-01T01:30:17Z" : "2026-09-17");
            if (seconds) {
                assert table.timeKey("2026-09-01T01:30:17Z") == key;
                assert table.timeKey("2106-02-07T06:28:15Z") == 0xffffffffL;
                try {
                    table.timeKey("2106-02-07T06:28:16Z");
                    throw new AssertionError("Second key overflow accepted");
                } catch (IllegalArgumentException expected) {
                    // All APIs must reject the first second beyond uint32.
                }
            } else {
                assert table.timeKey("20260918") == key + 1;
            }
            table.insert("AAPL", stamp, 1, 2, 0, 1, 3, 4, 1000000);
            table.insert("AAPL", "@" + (key + 1), 1, 2, 0, 1, 3, 4, 1000000);
            table.insert("AAPL", "@" + (key + 2), 1, 2, 0, 1, 3, 4, 1000000);
            table.insert("AAPL", "@4294967295", 1, 2, 0, 1, 3, 4, 1000000);
            try (Ohlc.Query query = table.series("AAPL", stamp, "@" + (key + 1))) {
                int rows = 0;
                Ohlc.Chunk chunk;
                while ((chunk = query.next()) != null) {
                    rows += chunk.count();
                }
                assert rows == 2;
            }
            long[][] ranges = {{key, key, 1}, {key + 3, key + 3, 0},
                               {0xffffffffL, 0xffffffffL, 1}};
            for (long[] range : ranges) {
                try (Ohlc.Query query = table.series(table.resolve("AAPL"), range[0], range[1])) {
                    int rows = 0;
                    Ohlc.Chunk chunk;
                    while ((chunk = query.next()) != null) {
                        rows += chunk.count();
                    }
                    assert rows == range[2];
                }
            }
            long[][] invalid = {{key + 1, key}, {key, 0x100000000L}, {key, -1}};
            for (long[] range : invalid) {
                try (Ohlc.Query query = table.series(table.resolve("AAPL"), range[0], range[1])) {
                    throw new AssertionError("Invalid closed range accepted: " + query);
                } catch (IllegalArgumentException expected) {
                    // Rejection must occur before a query takes ownership of the connection.
                }
            }
            try (Ohlc.Query query = table.series("AAPL", stamp, "@4294967296")) {
                throw new AssertionError("Out-of-range endpoint accepted: " + query);
            } catch (IllegalArgumentException expected) {
                // The former exclusive sentinel is not a valid inclusive time key.
            }
            client.drop("java_period");
        }
    }

    public static void main(String[] arguments) throws Exception {
        byte[] token = "test-writer".getBytes(StandardCharsets.US_ASCII);
        try (Ohlc client = Ohlc.unix(Path.of(arguments[0]), token, 30000)) {
            client.ping();
            client.checkpoint();
            Ohlc.Stats stats = client.stats();
            assert stats.commitSequence() == stats.checkpointSequence();
            assert stats.tableCount() == 2 && stats.tickerCount() == 3;
            Ohlc.Table table = client.table("bars_3m");
            long key = table.timeKey("20260901 09:30:00");
            assert key == table.timeKey("2026-09-01T01:30:00Z");
            assert table.timeKey(table.formatTime(0xffffffffL)) == 0xffffffffL;
            Ohlc.Table day = client.table("bars_5d");
            assert day.timeKey(day.formatTime(0xffffffffL)) == 0xffffffffL;
            assert client.tables(1, 128).size() == 2;
            extendedPeriods(client);
            nullableRows(client);
            Ohlc.Table removed = client.create("java_drop", 1, true, "", "");
            client.drop("java_drop");
            Ohlc.Table replacement = client.create("java_drop", 1, true, "", "");
            assert replacement.id > removed.id;
            assert client.tables(removed.id, 1).get(0).id == replacement.id;
            client.drop("java_drop");
            assert client.dictionary(1, 0, 128).size() == 2;
            long count = 0;
            try (Ohlc.Query query = table.series("AAPL", "20260901 09:30:00", "@4294967295")) {
                Ohlc.Chunk chunk;
                while ((chunk = query.next()) != null) {
                    ByteBuffer rows = chunk.data();
                    while (rows.hasRemaining()) {
                        assert Ohlc.u32(rows) == key + count * 3;
                        assert rows.getLong() == Long.MIN_VALUE;
                        assert rows.getLong() == Long.MAX_VALUE;
                        assert rows.getLong() == -1;
                        assert rows.getLong() == 0;
                        assert rows.getLong() == -1L;
                        assert Long.toUnsignedString(rows.getLong()).equals("18446744073709551615");
                        assert rows.getLong() == 0;
                        assert rows.getInt() == Integer.MIN_VALUE;
                        assert rows.getInt() == 255;
                        count++;
                    }
                }
            }
            assert count == 17000;
            try (Ohlc.Query query = table.cross(key)) {
                int rows = 0;
                Ohlc.Chunk chunk;
                while ((chunk = query.next()) != null) {
                    rows += chunk.count();
                }
                assert rows == 2;
            }
            Ohlc.Table java = client.create("java_day", 5, true, "", "Java contract check");
            long code = 0;
            ByteBuffer records = Ohlc.buffer(Ohlc.WRITE_BYTES);
            Ohlc.putWrite(records, code, java.timeKey("20260901"), Long.MIN_VALUE,
                          Long.MAX_VALUE, 0, -1, -1L, -1L, -1L, Integer.MAX_VALUE);
            java.write(new byte[][] {"JAVA".getBytes(StandardCharsets.UTF_8)}, records.flip());
            assert java.resolve("JAVA") == code;
            try (Ohlc.Query query = java.cross("20260901")) {
                Ohlc.Chunk chunk = query.next();
                assert chunk.count() == 1;
                assert chunk.data().getLong(44) == -1L;
                while (query.next() != null) {
                    // Consume successful FINAL before releasing the connection.
                }
            }
        }
        System.out.println("Java protocol, uint32/uint64, dates, streaming and writes: OK");
    }
}
