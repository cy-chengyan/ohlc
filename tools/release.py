# SPDX-License-Identifier: Apache-2.0
"""Create a deterministic source archive from the tracked files of a clean work tree."""

import argparse
import gzip
import hashlib
import io
import os
from pathlib import Path
import re
import subprocess
import tarfile

ROOT = Path(__file__).resolve().parents[1]


def require_clean_tree():
    """Refuse modified, staged, deleted or untracked files; ignored files do not count."""
    status = subprocess.run(["git", "status", "--porcelain", "--untracked-files=all"],
                            cwd=ROOT, check=True, capture_output=True).stdout
    entries = status.decode("utf-8", errors="replace").splitlines()
    if entries:
        listing = "\n".join(f"  {entry}" for entry in entries)
        raise SystemExit(f"Release requires a clean work tree; commit or remove:\n{listing}")


def source_files():
    paths = subprocess.run(["git", "ls-files", "--cached", "-z"],
                           cwd=ROOT, check=True, capture_output=True).stdout.split(b"\0")
    directories = {"src", "include", "clients", "examples", "tests", "tools", "cmake", "packaging"}
    metadata = {"CMakeLists.txt", "README.md", "README_CN.md", "LICENSE", ".clang-format",
                ".editorconfig"}
    for name in sorted(set(os.fsdecode(value) for value in paths if value)):
        path = Path(name)
        document = path.parts[0] == "docs" and path.suffix in (".md", ".svg", ".png")
        if name not in metadata and path.parts[0] not in directories and not document:
            continue
        absolute = ROOT / path
        if absolute.is_symlink() or not absolute.is_file():
            raise RuntimeError(f"Unexpected release entry: {name}")
        yield path, absolute.read_bytes(), bool(absolute.stat().st_mode & 0o111)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "dist")
    arguments = parser.parse_args()
    require_clean_tree()
    header = (ROOT / "include/ohlc/ohlc.h").read_text()
    version = re.search(r'^#define OHLC_VERSION "([0-9]+\.[0-9]+\.[0-9]+)"$', header, re.M).group(1)
    assert f'__version__ = "{version}"' in (ROOT / "clients/python/ohlc/__init__.py").read_text()
    assert f'VERSION = "{version}"' in (ROOT / "clients/java/io/ohlc/Ohlc.java").read_text()
    assert f"upstream_version {version}\n" in (ROOT / "packaging/ohlc.spec").read_text()
    epoch = int(os.environ.get("SOURCE_DATE_EPOCH", subprocess.run(
        ["git", "show", "-s", "--format=%ct", "HEAD"], cwd=ROOT, check=True,
        capture_output=True, text=True).stdout.strip()))
    files = list(source_files())
    manifest = "".join(f"{hashlib.sha256(data).hexdigest()}  {path.as_posix()}\n"
                       for path, data, _ in files).encode()
    files.append((Path("SOURCE_MANIFEST.sha256"), manifest, False))
    arguments.output.mkdir(parents=True, exist_ok=True)
    archive = arguments.output / f"ohlc-{version}.tar.gz"
    temporary = archive.with_suffix(".tmp")
    with temporary.open("wb") as raw:
        with gzip.GzipFile(fileobj=raw, mode="wb", filename="", mtime=epoch) as compressed:
            with tarfile.open(fileobj=compressed, mode="w", format=tarfile.PAX_FORMAT) as package:
                for path, data, executable in files:
                    info = tarfile.TarInfo(f"ohlc-{version}/{path.as_posix()}")
                    info.size = len(data)
                    info.mtime = epoch
                    info.mode = 0o755 if executable else 0o644
                    package.addfile(info, io.BytesIO(data))
    temporary.replace(archive)
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    archive.with_name(archive.name + ".sha256").write_text(f"{digest}  {archive.name}\n")
    print(f"{archive}: {len(files)} entries, SHA256 {digest}")


if __name__ == "__main__":
    main()
