#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Run as root on a disposable test host or systemd container with no OHLC install.
set -euo pipefail

if [[ $# != 4 || $(id -u) != 0 ]]; then
    echo "Usage (root): rpm_acceptance.sh PREDECESSOR_RPMS RELEASE_RPMS SOURCE WORK_DIR" >&2
    exit 2
fi
previous=$1
release=$2
source=$3
work=$4
case "$work" in /ssd02/*) ;; *) echo "Use a fresh directory under /ssd02" >&2; exit 2 ;; esac
if [[ -e "$work" ]] || rpm -q ohlc-libs >/dev/null 2>&1 || [[ -e /etc/ohlc ]]; then
    echo "Refusing to replace an existing installation or test directory" >&2
    exit 2
fi
packages=(ohlc-libs ohlc-client-libs ohlc-server ohlc-client ohlc-devel python3-ohlc ohlc-java ohlc-jni)
mkdir -m0700 "$work"

install_packages() {
    local directory=$1
    local name
    local file
    local matches
    local selected=()
    for name in "${packages[@]}"; do
        matches=0
        while IFS= read -r file; do
            if [[ $(rpm -qp --queryformat '%{NAME}' "$file") == "$name" ]]; then
                selected+=("$file")
                matches=$((matches + 1))
            fi
        done < <(find "$directory" -type f -name '*.rpm' ! -name '*debuginfo*' ! -name '*debugsource*')
        [[ $matches == 1 ]]
    done
    rpm -Uvh "${selected[@]}"
}

query() {
    runuser -u ohlc -- /usr/bin/ohlc --no-history --socket /run/ohlc/ohlcd.sock --execute "$1"
}

start_service() {
    systemctl daemon-reload
    systemctl start ohlc
    systemctl is-active --quiet ohlc
    query 'ping; stats;'
}

install_packages "$previous"
chown ohlc:ohlc "$work"
install -d -m0700 -o ohlc -g ohlc "$work/database"
sed -i "s|^data = .*|data = $work/database|; s/^memory-mib = .*/memory-mib = 256/" /etc/ohlc/ohlcd.conf
echo '# Acceptance configuration must survive an upgrade.' >> /etc/ohlc/ohlcd.conf
mkdir -p /etc/systemd/system/ohlc.service.d
printf '[Service]\nReadWritePaths=%s\n' "$work" > /etc/systemd/system/ohlc.service.d/90-beta-acceptance.conf
sha256sum /etc/ohlc/ohlcd.conf > "$work/config.sha256"
test "$(stat -c %a /etc/ohlc/ohlcd.conf)" = 640
start_service
test "$(stat -c %a /run/ohlc)" = 700
test "$(stat -c %a /run/ohlc/ohlcd.sock)" = 700
query 'create acceptance --period 3m --timezone UTC; register AAPL; insert acceptance AAPL "20260901 09:30:00" -2147483648 2147483647 -1 0 4294967295 18446744073709551615 4294967295;'
query 'cross acceptance "20260901 09:30:00" --format jsonl;' > "$work/before.jsonl"

old_pid=$(systemctl show -p MainPID --value ohlc)
systemctl kill --kill-who=main --signal=KILL ohlc
recovered=false
for ((attempt = 0; attempt < 90; attempt++)); do
    sleep 1
    new_pid=$(systemctl show -p MainPID --value ohlc)
    if [[ $new_pid != 0 && $new_pid != "$old_pid" ]] && systemctl is-active --quiet ohlc; then
        recovered=true
        break
    fi
done
[[ $recovered == true ]]
query 'cross acceptance "20260901 09:30:00" --format jsonl;' > "$work/after-crash.jsonl"
cmp "$work/before.jsonl" "$work/after-crash.jsonl"
echo 'PASS: install, readiness, private permissions, automatic restart and acknowledged rows'

install_packages "$release"
sha256sum -c "$work/config.sha256"
systemctl restart ohlc
query 'cross acceptance "20260901 09:30:00" --format jsonl;' > "$work/after-upgrade.jsonl"
cmp "$work/before.jsonl" "$work/after-upgrade.jsonl"
rpm -q "${packages[@]}"
ohlcd --version
ohlc --version
ohlc-admin --version
echo 'PASS: package upgrade preserves modified configuration and data'

# Consume installed development metadata without build-tree paths.
unset LD_LIBRARY_PATH PYTHONPATH OHLC_LIBRARY OHLC_CLIENT_LIBRARY
mkdir "$work/consumer"
cp "$source/examples/embedded.c" "$work/consumer/embedded.c"
cat > "$work/consumer/CMakeLists.txt" <<'CMAKE'
cmake_minimum_required(VERSION 3.20)
project(consumer LANGUAGES C)
find_package(ohlc CONFIG REQUIRED)
add_executable(consumer embedded.c)
target_link_libraries(consumer PRIVATE ohlc::ohlc ohlc::client)
CMAKE
cmake -S "$work/consumer" -B "$work/consumer/build"
cmake --build "$work/consumer/build" --parallel 2
"$work/consumer/build/consumer" "$work/cmake-db"
cc -std=c17 -Wall -Wextra -Werror "$source/examples/embedded.c" $(pkg-config --cflags --libs ohlc) -o "$work/c-consumer"
"$work/c-consumer" "$work/pkgconfig-db"
pkg-config --libs ohlc-client
python3.9 "$source/examples/embedded.py" "$work/python-db"
java_home=/usr/lib/jvm/java-17-openjdk
"$java_home/bin/javac" --release 17 -Xlint:all -Werror -cp /usr/share/ohlc/ohlc-client.jar -d "$work/consumer" "$source/examples/Embedded.java"
"$java_home/bin/java" -Xcheck:jni -Djava.library.path=/usr/lib64 -cp "$work/consumer:/usr/share/ohlc/ohlc-client.jar" Embedded "$work/java-db"
python3.9 - <<'PY'
from ohlc import Connection, __version__
assert __version__ == "0.1.0b1"
with Connection(socket="/run/ohlc/ohlcd.sock") as connection:
    assert connection.stats()["table_count"] == 1
    connection.checkpoint()
PY
for file in /usr/bin/ohlcd /usr/bin/ohlc /usr/bin/ohlc-admin /usr/lib64/libohlc.so.0 /usr/lib64/libohlc_client.so.0 /usr/lib64/libohlc_jni.so; do
    if readelf -d "$file" | grep -E '\((RPATH|RUNPATH)\)'; then
        echo "Unexpected runtime path in $file" >&2
        exit 1
    fi
done
echo 'PASS: installed C, Python, Java, JNI, pkg-config and CMake consumption'

systemctl stop ohlc
runuser -u ohlc -- ohlc-admin check "$work/database"
runuser -u ohlc -- ohlc-admin backup "$work/database" "$work/archive.ohlc"
runuser -u ohlc -- ohlc-admin restore "$work/archive.ohlc" "$work/restored"
runuser -u ohlc -- ohlc-admin check "$work/restored"
printf 'retain\n' > /var/lib/ohlc/beta-retention-marker
rpm -e "${packages[@]}"
test -s "$work/database/catalog-000001.dat"
test -s /var/lib/ohlc/beta-retention-marker
test -f /etc/ohlc/ohlcd.conf.rpmsave
getent passwd ohlc
echo 'PASS: removal preserves database, nonempty state directory and modified configuration'

install_packages "$release"
cp /etc/ohlc/ohlcd.conf.rpmsave /etc/ohlc/ohlcd.conf
chown root:ohlc /etc/ohlc/ohlcd.conf
chmod 0640 /etc/ohlc/ohlcd.conf
start_service
query 'cross acceptance "20260901 09:30:00" --format jsonl;' > "$work/after-reinstall.jsonl"
cmp "$work/before.jsonl" "$work/after-reinstall.jsonl"
systemctl stop ohlc
journalctl -u ohlc --no-pager > "$work/journal.log"
rpm -e "${packages[@]}"
# Remove only configuration artifacts and marker created by this test.
rm -f /var/lib/ohlc/beta-retention-marker /etc/systemd/system/ohlc.service.d/90-beta-acceptance.conf
rm -f /etc/ohlc/ohlcd.conf.rpmsave
rmdir /etc/systemd/system/ohlc.service.d /etc/ohlc /var/lib/ohlc 2>/dev/null || true
systemctl daemon-reload
echo 'PASS: reinstall opens retained data; all package lifecycle checks complete'
