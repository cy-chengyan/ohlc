#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Install official competitor archives in a private user directory, without services."""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tarfile


def fetch(url, destination):
    subprocess.run(["curl", "--fail", "--location", "--silent", "--show-error",
                    "--connect-timeout", "30", "--max-time", "600", "--output", str(destination),
                    url], check=True)
    digest = hashlib.sha256()
    with destination.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return {"url": url, "sha256": digest.hexdigest(), "archive": str(destination)}


def install(root):
    root.mkdir(parents=True, exist_ok=True)
    manifest_path = root / "install.json"
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    urls = {
        "clickhouse": "https://github.com/ClickHouse/ClickHouse/releases/download/"
                      "v26.10.2.103-stable/clickhouse-common-static-26.10.2.103-amd64.tgz",
        "influxdb": "https://dl.influxdata.com/influxdb/releases/v2.7.12/"
                    "influxdb2-2.7.12_linux_amd64.tar.gz",
        "aerospike": "https://download.aerospike.com/artifacts/aerospike-server-community/8.2.0.1/"
                     "aerospike-server-community_8.2.0.1_tools-13.1.0_el9_x86_64.tgz",
    }
    for name in ("clickhouse", "influxdb", "aerospike"):
        if manifest.get(name, {}).get("status") == "installed":
            continue
        try:
            url = urls[name]
            destination = root / name
            destination.mkdir(exist_ok=True)
            archive = destination / "release.tgz"
            record = fetch(url, archive)
            if name == "influxdb" and record["sha256"] != (
                    "825641e678b4a0f6e2094293f326b472269f30c3d02a989bee8c37bfa706f8d7"):
                raise RuntimeError("InfluxDB archive differs from its official release digest")
            with tarfile.open(archive) as source:
                source.extractall(destination, filter="data")
            if name == "aerospike":
                rpm = next(destination.rglob("aerospike-server-community-*.rpm"))
                unpack = destination / "unpack"
                unpack.mkdir(exist_ok=True)
                with subprocess.Popen(["rpm2cpio", str(rpm)], stdout=subprocess.PIPE) as producer:
                    subprocess.run(["cpio", "-id", "--quiet"], stdin=producer.stdout,
                                   cwd=unpack, check=True)
                    producer.stdout.close()
                    if producer.wait():
                        raise RuntimeError("rpm2cpio failed")
                binary = next(unpack.rglob("asd"))
            else:
                binary = next(path for path in destination.rglob(
                    "clickhouse" if name == "clickhouse" else "influxd") if path.is_file())
            version = subprocess.check_output([str(binary), "--version" if name != "influxdb"
                                               else "version"], text=True, stderr=subprocess.STDOUT,
                                              cwd=root)
            record.update(status="installed", binary=str(binary), version=version.strip(),
                          binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest())
            manifest[name] = record
        except Exception as error:
            manifest[name] = {"status": "incomplete", "reason": str(error)}
        manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    arguments = parser.parse_args()
    print(json.dumps(install(arguments.root), indent=2))
