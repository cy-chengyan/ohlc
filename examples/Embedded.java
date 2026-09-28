// SPDX-License-Identifier: Apache-2.0
import io.ohlc.Database;
import io.ohlc.Ohlc;
import java.nio.ByteBuffer;
import java.nio.file.Path;

/** Use an unused directory; load libohlc_jni through java.library.path. */
public final class Embedded {
    private Embedded() {}

    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 1) {
            throw new IllegalArgumentException("Usage: Embedded DATABASE_DIRECTORY");
        }
        try (Database db = Database.open(Path.of(arguments[0]), true, Database.Options.defaults())) {
            Database.Table table = db.create("bars_3m", 3, false, "Asia/Shanghai", "Example bars");
            long ticker = table.register("AAPL").code();
            ByteBuffer batch = Database.buffer(2 * Ohlc.WRITE_BYTES);
            Ohlc.putWrite(batch, ticker, table.timeKey("20260901 09:30:00"),
                          10000, 10100, 9950, 10080, 1200, 12100000, 1000000);
            Ohlc.putWrite(batch, ticker, table.timeKey("20260901 09:33:00"),
                          10080, 10120, 10000, 10100, 900, 9100000, 1000000);
            table.write(batch.flip());
            // Reuse this direct buffer across query chunks.
            ByteBuffer output = Database.buffer(256 * Ohlc.RESULT_BYTES);
            try (Database.Query query = table.series("AAPL", "20260901 09:30:00",
                                                     "20260901 09:33:00")) {
                while (query.read(output.clear()) != 0) {
                    output.flip();
                    while (output.hasRemaining()) {
                        long time = Ohlc.u32(output);
                        int open = output.getInt();
                        output.position(output.position() + Ohlc.ROW_BYTES - Integer.BYTES);
                        System.out.println(table.formatTime(time) + " open=" + open);
                    }
                }
            }
            try (Database.Query query = table.cross("20260901 09:30:00")) {
                Database.Chunk chunk = query.next();
                System.out.println("Cross rows: " + (chunk == null ? 0 : chunk.count()));
            }
            db.checkpoint();
        }
    }
}
