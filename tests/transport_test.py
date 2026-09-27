# SPDX-License-Identifier: Apache-2.0
"""Verify TLS identity and non-retryable mutation/partial-stream boundaries."""

import os
from pathlib import Path
import select
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "clients" / "python"))
from ohlc import Connection, Error, OutcomeUnknown
from network_test import connect_when_ready, require_error


def frame(operation, request, body=b"", flags=3, status=0, chunk=0):
    return struct.pack("<4sHHIIQII", b"OHLC", 3, operation, flags, len(body), request,
                       status, chunk) + body


def receive(peer):
    def exact(size):
        data = bytearray()
        while len(data) < size:
            part = peer.recv(size - len(data))
            if not part:
                raise EOFError("Truncated request")
            data.extend(part)
        return data
    header = struct.unpack("<4sHHIIQII", exact(32))
    return header[2], header[5], exact(header[4])


def fake_server(handler):
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(10)
    port = listener.getsockname()[1]
    errors = []
    def serve():
        try:
            with listener:
                peer, _ = listener.accept()
                with peer:
                    peer.settimeout(5)
                    operation, request, _ = receive(peer)
                    assert operation == 1
                    hello = b"\x01" * 16 + struct.pack("<IIII", 16 << 20, 250000, 30000, 3)
                    peer.sendall(frame(1, request, hello))
                    handler(peer)
        except BaseException as error:
            errors.append(error)
    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    return port, thread, errors


def wait_fake(thread, errors):
    thread.join(10)
    assert not thread.is_alive()
    assert not errors, errors


def protocol_failures(library, shell):
    def lost_ack(peer):
        operation, _, _ = receive(peer)
        assert operation == 7
    port, thread, errors = fake_server(lost_ack)
    with Connection(port=port, library=library) as client:
        try:
            client.register("MAY_HAVE_COMMITTED")
        except OutcomeUnknown as error:
            assert error.code == 9
        else:
            raise AssertionError("Lost acknowledgement must be OUTCOME_UNKNOWN")
    wait_fake(thread, errors)
    if len(sys.argv) > 3:
        port, thread, errors = fake_server(lost_ack)
        java = sys.argv[4] if len(sys.argv) > 4 else "java"
        subprocess.run([java, "-ea", "-cp", sys.argv[3], "JavaTransportTest", "unknown", str(port)],
                       check=True, timeout=10)
        wait_fake(thread, errors)
    port, thread, errors = fake_server(lost_ack)
    result = subprocess.run([shell, "--port", str(port), "--execute",
                             'register "MAY_HAVE_COMMITTED"; register "MUST_NOT_RUN";'],
                            capture_output=True, text=True, timeout=10)
    assert result.returncode == 4, result.stderr
    wait_fake(thread, errors)

    def partial_query(peer):
        operation, request, _ = receive(peer)
        assert operation == 10
        definition = struct.pack("<I", 4) + b"bars" + struct.pack("<III", 1, 1, 3) + b"UTC" + struct.pack("<I", 0)
        peer.sendall(frame(10, request, struct.pack("<IQ", 1, 1) + definition))
        operation, request, _ = receive(peer)
        assert operation == 6
        body = struct.pack("<QQIIII", 1, 1, 1, 2, 1, 0) + struct.pack("<IiiiiIQI", 0, 1, 2, 3, 4, 5, 6, 7)
        peer.sendall(frame(6, request, body, flags=1))
        # EOF without FINAL must never convert a partial result into success.
    port, thread, errors = fake_server(partial_query)
    with Connection(port=port, library=library) as client:
        with client.table("bars").cross(1) as query:
            assert next(query).count == 1
            require_error(5, lambda: next(query))
    wait_fake(thread, errors)

    def wrong_sequence(peer):
        operation, request, _ = receive(peer)
        assert operation == 2
        peer.sendall(frame(2, request, chunk=1))
    port, thread, errors = fake_server(wrong_sequence)
    with Connection(port=port, library=library) as client:
        require_error(6, client.ping)
    wait_fake(thread, errors)


def terminal_check(shell, socket_path, token, root):
    import pty
    master, slave = pty.openpty()
    environment = dict(os.environ, XDG_STATE_HOME=str(root))
    process = subprocess.Popen([shell, "--socket", socket_path, "--token-file", str(token)],
                               stdin=slave, stdout=slave, stderr=slave, env=environment,
                               start_new_session=True)
    os.close(slave)
    collected = bytearray()
    def until(marker):
        deadline = time.monotonic() + 10
        while marker not in collected:
            if time.monotonic() >= deadline:
                raise TimeoutError(bytes(collected[-4000:]))
            if select.select([master], [], [], 0.2)[0]:
                collected.extend(os.read(master, 65536))
        position = collected.index(marker) + len(marker)
        del collected[:position]
    try:
        until(b"ohlc> ")
        os.write(master, b"hel\t\n")
        until(b"Uncertain writes are never automatically retried.")
        until(b"ohlc> ")
        os.write(master, b"discard this\x03")
        until(b"ohlc> ")
        os.write(master, b"create interactive --period 3m --timezone Asia/Shanghai;\n")
        until(b"Created commit_seq=")
        until(b"ohlc> ")
        os.write(master, b"describe inter\t;\n")
        until(b"6 adjust_factor uint32")
        until(b"ohlc> ")
        os.write(master, b"quit;\n")
        process.wait(timeout=10)
        assert process.returncode == 0
        history = root / "ohlc-history"
        assert history.exists() and history.stat().st_mode & 0o777 == 0o600
        assert b"test-writer" not in history.read_bytes()
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
        os.close(master)


def main():
    server = os.path.abspath(sys.argv[1])
    library = os.path.abspath(sys.argv[2])
    shell = str(Path(server).with_name("ohlc"))
    protocol_failures(library, shell)
    with tempfile.TemporaryDirectory(prefix="ohlc-tls-", dir=os.environ.get("TMPDIR", "/tmp")) as directory:
        root = Path(directory)
        certificate = root / "cert.pem"
        private_key = root / "key.pem"
        configuration = root / "cert.conf"
        configuration.write_text("[req]\nprompt=no\ndistinguished_name=dn\nx509_extensions=ext\n"
                                  "[dn]\nCN=localhost\n[ext]\nsubjectAltName=DNS:localhost\n"
                                  "basicConstraints=critical,CA:TRUE\n")
        subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                        "-config", str(configuration), "-keyout", str(private_key),
                        "-out", str(certificate)], check=True, capture_output=True, timeout=20)
        token = root / "token"
        token.write_bytes(b"test-writer\n")
        token.chmod(0o600)
        with socket.socket() as reserve:
            reserve.bind(("127.0.0.1", 0))
            port = reserve.getsockname()[1]
        arguments = [server, "--data", str(root / "tls-db"), "--port", str(port),
                     "--tls-cert", str(certificate), "--tls-key", str(private_key),
                     "--write-token-file", str(token)]
        process = subprocess.Popen(arguments, stderr=subprocess.PIPE)
        try:
            options = dict(host="localhost", port=port, tls=True, ca_file=certificate,
                           token=b"test-writer", library=library)
            with connect_when_ready(process, **options) as client:
                client.ping()
                table = client.create("secure", period="1d")
                client.register("TLS")
                table.insert("TLS", "20260901", (1, 2, 3, 4, 5, 6, 7))
                with table.cross("20260901") as query:
                    assert sum(chunk.count for chunk in query) == 1
            # The chain is trusted here, but the IP does not match DNS:localhost.
            require_error(5, lambda: Connection(**dict(options, host="127.0.0.1")))
            require_error(5, lambda: Connection(**dict(options, ca_file=None)))
            with Connection(**options) as client:
                client.ping()
            subprocess.run([shell, "--host", "localhost", "--port", str(port), "--tls",
                            "--ca", str(certificate), "--token-file", str(token),
                            "--execute", 'cross secure "20260901";'],
                           check=True, capture_output=True, timeout=10)
            if len(sys.argv) > 3:
                java = sys.argv[4] if len(sys.argv) > 4 else "java"
                subprocess.run([java, "-ea", "-cp", sys.argv[3], "JavaTransportTest", "tls",
                                str(port), str(certificate)], check=True, timeout=15)
        finally:
            process.send_signal(signal.SIGTERM)
            _, error = process.communicate(timeout=20)
            assert process.returncode == 0, error.decode()
        socket_path = str(root / "socket")
        process = subprocess.Popen([server, "--data", str(root / "terminal-db"), "--socket",
                                    socket_path, "--write-token-file", str(token)], stderr=subprocess.PIPE)
        try:
            with connect_when_ready(process, socket=socket_path, token=b"test-writer", library=library):
                pass
            terminal_check(shell, socket_path, token, root)
        finally:
            process.send_signal(signal.SIGTERM)
            _, error = process.communicate(timeout=20)
            assert process.returncode == 0, error.decode()
    print("TLS trust/identity, partial results, unknown outcomes and real terminal: OK")


if __name__ == "__main__":
    main()
