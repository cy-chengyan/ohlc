"""Check the versioned C formatting contract without changing source files."""

from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
FORMAT_VERSION = "21.1.8"


def main():
    formatter = sys.argv[1] if len(sys.argv) > 1 else "clang-format"
    version = subprocess.run(
        [formatter, "--version"], check=True, capture_output=True, text=True
    ).stdout
    if f"version {FORMAT_VERSION}" not in version:
        raise SystemExit(f"Expected clang-format {FORMAT_VERSION}: {version.strip()}")
    files = []
    for directory in ("include", "src", "tests", "examples"):
        for path in sorted((ROOT / directory).rglob("*")):
            if path.suffix in (".c", ".h"):
                files.append(str(path))
    subprocess.run([formatter, "--dry-run", "--Werror", *files], check=True)


if __name__ == "__main__":
    main()
