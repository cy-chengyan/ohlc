// SPDX-License-Identifier: Apache-2.0
import io.ohlc.Ohlc;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;

public final class JavaClientTest {
    private JavaClientTest() {}

    public static void main(String[] arguments) throws Exception {
        byte[] token = "test-writer".getBytes(StandardCharsets.US_ASCII);
        try (Ohlc client = Ohlc.unix(Path.of(arguments[0]), token, 30000)) {
            client.ping();
            client.checkpoint();
            Ohlc.Stats stats = client.stats();
            assert stats.commitSequence() == stats.checkpointSequence();
            assert stats.tableCount() == 2 && stats.tickerCount() == 2;
            Ohlc.Table table = client.table("bars_3m");
            long key = table.timeKey("20260901 09:30:00");
            assert key == table.timeKey("2026-09-01T01:30:00Z");
            assert table.timeKey(table.formatTime(0xffffffffL)) == 0xffffffffL;
            Ohlc.Table day = client.table("bars_5d");
            assert day.timeKey(day.formatTime(0xffffffffL)) == 0xffffffffL;
            assert client.tables(1, 128).size() == 2;
            Ohlc.Table removed = client.create("java_drop", 1, true, "", "");
            client.drop("java_drop");
            Ohlc.Table replacement = client.create("java_drop", 1, true, "", "");
            assert replacement.id > removed.id;
            assert client.tables(removed.id, 1).get(0).id == replacement.id;
            client.drop("java_drop");
            assert client.dictionary(0, 128).size() == 2;
            long count = 0;
            try (Ohlc.Query query = table.series("AAPL", "20260901 09:30:00", "@4294967296")) {
                Ohlc.Chunk chunk;
                while ((chunk = query.next()) != null) {
                    ByteBuffer rows = chunk.data();
                    while (rows.hasRemaining()) {
                        assert Ohlc.u32(rows) == key + count * 3;
                        assert rows.getInt() == Integer.MIN_VALUE;
                        assert rows.getInt() == Integer.MAX_VALUE;
                        assert rows.getInt() == -1;
                        assert rows.getInt() == 0;
                        assert Ohlc.u32(rows) == 0xffffffffL;
                        assert Long.toUnsignedString(rows.getLong()).equals("18446744073709551615");
                        assert Ohlc.u32(rows) == 0;
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
            long code = client.register("JAVA").code();
            ByteBuffer records = Ohlc.buffer(40);
            Ohlc.putWrite(records, code, java.timeKey("20260901"), Integer.MIN_VALUE,
                          Integer.MAX_VALUE, 0, -1, 0xffffffffL, -1L, 0xffffffffL);
            java.write(records.flip());
            try (Ohlc.Query query = java.cross("20260901")) {
                Ohlc.Chunk chunk = query.next();
                assert chunk.count() == 1;
                assert chunk.data().getLong(24) == -1L;
                while (query.next() != null) {
                    // Consume successful FINAL before releasing the connection.
                }
            }
        }
        System.out.println("Java protocol, uint32/uint64, dates, streaming and writes: OK");
    }
}
