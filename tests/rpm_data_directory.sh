#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Run after installing the RPMs in a fresh, disposable systemd container only.
set -euo pipefail

if [[ $(id -u) != 0 ]] || ! systemd-detect-virt --container --quiet; then
    echo 'This test requires root in a disposable systemd container.' >&2
    exit 2
fi
if systemctl is-active --quiet ohlc || [[ -e /var/lib/ohlc/database || -e /data01 ]]; then
    echo 'Refusing to replace existing service data.' >&2
    exit 2
fi
rpm -q ohlc-server ohlc-client
systemctl daemon-reload
test "$(systemctl show ohlc -p ProtectSystem --value)" = full
test -z "$(systemctl show ohlc -p ReadWritePaths --value)"

query() {
    runuser -u ohlc -- ohlc --no-history --socket /run/ohlc/ohlcd.sock --execute "$1"
}

trap 'systemctl stop ohlc' EXIT
systemctl start ohlc
query 'ping; stats;'
systemctl stop ohlc
echo 'PASS: default data directory starts with the packaged service'

install -d -m0755 /data01
install -d -m0700 -o ohlc -g ohlc /data01/ohlc /data01/ohlc/database
sed -i 's|^data = .*|data = /data01/ohlc/database|' /etc/ohlc/ohlcd.conf
install -d -m0755 /etc/systemd/system/ohlc.service.d
cat > /etc/systemd/system/ohlc.service.d/90-data-test.conf <<'EOF'
[Service]
ProtectSystem=strict
Restart=no
EOF
systemctl daemon-reload
runuser -u ohlc -- ohlcd --config /etc/ohlc/ohlcd.conf --check-config
if systemctl start ohlc; then
    echo 'ProtectSystem=strict unexpectedly allowed the custom data directory.' >&2
    exit 1
fi
test "$(systemctl show ohlc -p ExecMainStatus --value)" = 1
journalctl -u ohlc --no-pager | grep -F 'Open database: I/O error'
test ! -e /data01/ohlc/database/LOCK
echo 'PASS: strict reproduces the custom directory startup failure'

# Retain only the retry override; the packaged unit supplies ProtectSystem=full.
printf '[Service]\nRestart=no\n' > /etc/systemd/system/ohlc.service.d/90-data-test.conf
systemctl daemon-reload
systemctl reset-failed ohlc
test "$(systemctl show ohlc -p ProtectSystem --value)" = full
test -z "$(systemctl show ohlc -p ReadWritePaths --value)"
systemctl start ohlc
query 'create acceptance --period 1d; insert acceptance TEST "20261011" 1 2 1 2 3 4 5;'
query 'cross acceptance "20261011" --format jsonl;' > /tmp/data-before.jsonl
test -s /tmp/data-before.jsonl
systemctl restart ohlc
query 'cross acceptance "20261011" --format jsonl;' > /tmp/data-after.jsonl
cmp /tmp/data-before.jsonl /tmp/data-after.jsonl
test "$(stat -c %U:%G:%a /data01/ohlc/database)" = ohlc:ohlc:700
echo 'PASS: full supports custom data, writes, queries and restart without ReadWritePaths'

systemctl stop ohlc
chown root:root /data01/ohlc/database
if systemctl start ohlc; then
    echo 'A root-owned private data directory unexpectedly started.' >&2
    exit 1
fi
test "$(systemctl show ohlc -p ExecMainStatus --value)" = 1
chown ohlc:ohlc /data01/ohlc/database
systemctl reset-failed ohlc
systemctl start ohlc
query 'cross acceptance "20261011" --format jsonl;' > /tmp/data-restored.jsonl
cmp /tmp/data-before.jsonl /tmp/data-restored.jsonl
echo 'PASS: full retains service account permission checks'
