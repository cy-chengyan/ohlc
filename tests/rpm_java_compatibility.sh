#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Validate the installed JAR and JNI bridge with the selected JDK in a container.
set -euo pipefail

if [[ $# != 1 ]] || ! systemd-detect-virt --container --quiet; then
    echo 'Usage in a disposable container: rpm_java_compatibility.sh JDK_HOME' >&2
    exit 2
fi
jdk=$1
source=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
work=$(mktemp -d /tmp/ohlc-java.XXXXXX)
trap 'rm -rf -- "$work"' EXIT
jar=/usr/share/ohlc/ohlc-client.jar
"$jdk/bin/java" -version
"$jdk/bin/javac" -version
rpm -q --requires ohlc-java
python3.9 - "$jar" <<'PY'
import struct
import sys
import zipfile

with zipfile.ZipFile(sys.argv[1]) as jar:
    classes = [name for name in jar.namelist() if name.endswith('.class')]
    assert classes, 'The client JAR must contain classes.'
    for name in classes:
        magic, minor, major = struct.unpack('>IHH', jar.read(name)[:8])
        assert (magic, minor, major) == (0xcafebabe, 0, 61), name
    print(f'PASS: all {len(classes)} classes use Java 17 bytecode without preview features')
PY

"$jdk/bin/javac" --release 17 -Xlint:all -Werror -cp "$jar" -d "$work" \
    "$source/tests/JavaClientTest.java" "$source/tests/JavaTransportTest.java" \
    "$source/tests/JavaEmbeddedTest.java" "$source/examples/Embedded.java"
classes="$work:$jar"
python3.9 "$source/tests/network_test.py" /usr/bin/ohlcd /usr/lib64/libohlc_client.so \
    "$classes" "$jdk/bin/java"
python3.9 "$source/tests/transport_test.py" /usr/bin/ohlcd /usr/lib64/libohlc_client.so \
    "$classes" "$jdk/bin/java"
# The embedded harness expects the admin tool beside the supplied library.
ln -s /usr/lib64/libohlc.so "$work/libohlc.so"
ln -s /usr/bin/ohlc-admin "$work/ohlc-admin"
python3.9 "$source/tests/embedded_test.py" "$work/libohlc.so" \
    /usr/lib64/libohlc_jni.so "$classes" "$jdk/bin/java"
"$jdk/bin/java" -Xcheck:jni -Djava.library.path=/usr/lib64 -cp "$classes" \
    Embedded "$work/example-db"
echo 'PASS: installed JAR supports development, network, TLS and JNI with the selected JDK'
