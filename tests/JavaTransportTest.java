// SPDX-License-Identifier: Apache-2.0
import io.ohlc.Ohlc;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

public final class JavaTransportTest {
    private JavaTransportTest() {}

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
