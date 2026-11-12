// SPDX-License-Identifier: Apache-2.0
import io.ohlc.Ohlc;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

public final class JavaTransportTest {
    private JavaTransportTest() {}

    private static final byte[] MAGIC = {'O', 'H', 'L', 'C'};

    private record Request(int operation, long id) {}

    /** Server behaviour after the fake HELLO reply. */
    private interface Script {
        void run(DataInputStream input, OutputStream output) throws IOException;
    }

    private static byte[] frame(byte[] magic, int version, int operation, long request,
                                byte[] body) {
        ByteBuffer frame = ByteBuffer.allocate(32 + body.length).order(ByteOrder.LITTLE_ENDIAN);
        frame.put(magic).putShort((short) version).putShort((short) operation).putInt(3)
             .putInt(body.length).putLong(request).putInt(0).putInt(0).put(body);
        return frame.array();
    }

    private static Request receive(DataInputStream input) throws IOException {
        byte[] header = new byte[32];
        input.readFully(header);
        ByteBuffer fields = ByteBuffer.wrap(header).order(ByteOrder.LITTLE_ENDIAN);
        input.readFully(new byte[fields.getInt(12)]);
        return new Request(Short.toUnsignedInt(fields.getShort(6)), fields.getLong(16));
    }

    /** Accept one client and answer its HELLO with the given frame magic and version. */
    private static Thread fakeServer(ServerSocket listener, byte[] magic, int version,
                                     Script script) {
        Thread thread = new Thread(() -> {
            try (ServerSocket server = listener) {
                try (Socket peer = server.accept()) {
                    DataInputStream input = new DataInputStream(peer.getInputStream());
                    OutputStream output = peer.getOutputStream();
                    Request hello = receive(input);
                    ByteBuffer body = ByteBuffer.allocate(32).order(ByteOrder.LITTLE_ENDIAN);
                    body.put(new byte[16]).putInt(16 << 20).putInt(200000).putInt(30000).putInt(3);
                    output.write(frame(magic, version, 1, hello.id(), body.array()));
                    script.run(input, output);
                }
            } catch (IOException ignored) {
                // The client ends each check by closing its connection.
            }
        });
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    private static void rejectHello(byte[] magic, int version, int status) throws Exception {
        ServerSocket listener = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
        Thread server = fakeServer(listener, magic, version, (input, output) -> {});
        try {
            Ohlc.connect("127.0.0.1", listener.getLocalPort(), null, new byte[0], 5000).close();
            throw new AssertionError("Invalid HELLO frame was accepted");
        } catch (Ohlc.Failure error) {
            assert error.status == status : "HELLO frame status " + error.status;
        }
        server.join(5000);
    }

    private static void closeAbortsBlockedQuery() throws Exception {
        ServerSocket listener = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
        Thread server = fakeServer(listener, MAGIC, 6, (input, output) -> {
            Request lookup = receive(input);
            ByteBuffer table = ByteBuffer.allocate(36).order(ByteOrder.LITTLE_ENDIAN);
            table.putInt(1).putLong(1).putInt(4).put("bars".getBytes(StandardCharsets.US_ASCII))
                 .putInt(2).putInt(1).putInt(0).putInt(0);
            output.write(frame(MAGIC, 6, lookup.operation(), lookup.id(), table.array()));
            receive(input);
            while (input.read() >= 0) {
                // Never answer the query; wait for the client to close.
            }
        });
        Ohlc client = Ohlc.connect("127.0.0.1", listener.getLocalPort(), null, new byte[0], 5000);
        Ohlc.Query query = client.table("bars").cross("20260901");
        AtomicReference<Throwable> outcome = new AtomicReference<>();
        Thread reader = new Thread(() -> {
            try {
                query.next();
            } catch (Throwable error) {
                outcome.set(error);
            }
        });
        reader.setDaemon(true);
        reader.start();
        Thread.sleep(300);
        long started = System.nanoTime();
        client.close();
        reader.join(2000);
        long elapsedMs = (System.nanoTime() - started) / 1_000_000;
        assert elapsedMs < 2000 && !reader.isAlive() : "close must abort a blocked query";
        assert outcome.get() instanceof IOException : "Blocked next() result " + outcome.get();
        server.join(5000);
    }

    private static void checkQuery(Ohlc client, String table) throws Exception {
        client.ping();
        try (Ohlc.Query query = client.table(table).cross("20260901")) {
            int count = 0;
            Ohlc.Chunk chunk;
            while ((chunk = query.next()) != null) {
                count += chunk.count();
            }
            assert count == 1;
        }
    }

    public static void main(String[] arguments) throws Exception {
        int port = Integer.parseInt(arguments[1]);
        if (arguments[0].equals("unknown")) {
            try (Ohlc client = Ohlc.connect("127.0.0.1", port, null, new byte[0], 5000)) {
                try {
                    client.register(1, "MAY_HAVE_COMMITTED");
                    throw new AssertionError("Expected unknown mutation outcome");
                } catch (Ohlc.Failure error) {
                    assert error.status == 9;
                }
            }
            rejectHello(MAGIC, 7, 7);
            rejectHello("XHLC".getBytes(StandardCharsets.US_ASCII), 6, 6);
            closeAbortsBlockedQuery();
            return;
        }
        byte[] token = "test-writer".getBytes(StandardCharsets.US_ASCII);
        if (arguments[0].equals("plain")) {
            String host = arguments[2];
            boolean authenticated = arguments[3].equals("token");
            byte[] credential = authenticated ? token : new byte[0];
            try (Ohlc client = Ohlc.connect(host, port, null, credential, 5000)) {
                checkQuery(client, "plain");
            }
            if (authenticated) {
                boolean rejected = false;
                byte[] wrong = "wrong".getBytes(StandardCharsets.US_ASCII);
                try (Ohlc client = Ohlc.connect(host, port, null, wrong, 5000)) {
                    client.ping();
                } catch (Ohlc.Failure expected) {
                    assert expected.status == 8;
                    rejected = true;
                }
                assert rejected : "Plain TCP must reject an incorrect token";
            }
            System.out.println("Java plain TCP " + arguments[3] + " query: OK");
            return;
        }
        Certificate certificate;
        try (InputStream stream = Files.newInputStream(Path.of(arguments[2]))) {
            certificate = CertificateFactory.getInstance("X.509").generateCertificate(stream);
        }
        KeyStore store = KeyStore.getInstance(KeyStore.getDefaultType());
        store.load(null, null);
        store.setCertificateEntry("test-ca", certificate);
        TrustManagerFactory factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        factory.init(store);
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(null, factory.getTrustManagers(), null);
        try (Ohlc client = Ohlc.connect("localhost", port, context, token, 5000)) {
            checkQuery(client, "secure");
        }
        boolean rejected = false;
        try (Ohlc client = Ohlc.connect("127.0.0.1", port, context, token, 5000)) {
            client.ping();
        } catch (java.io.IOException expected) {
            rejected = true;
        }
        assert rejected : "Trusted certificate with wrong host must be rejected";
        System.out.println("Java TLS identity and query: OK");
    }
}
