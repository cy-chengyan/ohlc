# SPDX-License-Identifier: Apache-2.0
"""Exercise interactive editing through a real PTY without a database server."""

import fcntl
import os
from pathlib import Path
import pty
import re
import select
import struct
import subprocess
import sys
import tempfile
import termios
import time


class Terminal:
    def __init__(self, shell, root, columns=80, no_history=False):
        self.master, self.slave = pty.openpty()
        self.original = termios.tcgetattr(self.slave)
        self.columns = columns
        fcntl.ioctl(self.slave, termios.TIOCSWINSZ, struct.pack("HHHH", 24, columns, 0, 0))
        arguments = [shell]
        if no_history:
            arguments.append("--no-history")
        self.process = subprocess.Popen(arguments, stdin=self.slave, stdout=self.slave,
                                        stderr=self.slave, start_new_session=True,
                                        env=dict(os.environ, XDG_STATE_HOME=str(root)))
        self.pending = bytearray()
        self.transcript = bytearray()
        self.until(b"ohlc> ")

    def send(self, keys):
        os.write(self.master, keys)

    def until(self, marker):
        deadline = time.monotonic() + 5
        while marker not in self.pending:
            if time.monotonic() >= deadline:
                raise TimeoutError((marker, bytes(self.pending[-3000:])))
            if select.select([self.master], [], [], 0.1)[0]:
                chunk = os.read(self.master, 65536)
                self.pending.extend(chunk)
                self.transcript.extend(chunk)
        end = self.pending.index(marker) + len(marker)
        del self.pending[:end]

    def preview(self, keys, expected):
        self.send(keys + b"\x05; help set;\n")
        self.expect_preview(expected)

    def expect_preview(self, expected):
        # Match command output, not the editor's repeated echoes of the input.
        self.until(b"Current settings")
        self.until(f"{expected} rows\r\n".encode())
        self.until(b"ohlc> ")

    def close(self):
        self.send(b"\x04")
        self.process.wait(timeout=5)
        assert self.process.returncode == 0
        restored = termios.tcgetattr(self.slave)
        # BSD sets PENDIN when canonical input resumes; it is transient kernel state.
        restored[3] &= ~getattr(termios, "PENDIN", 0)
        original = self.original.copy()
        original[3] &= ~getattr(termios, "PENDIN", 0)
        assert restored == original, (restored, original)

    def cleanup(self):
        if self.process.poll() is None:
            self.process.kill()
            self.process.wait()
        os.close(self.master)
        os.close(self.slave)


def editing_check(shell, root):
    history = root / "ohlc-history"
    history.write_bytes(b"set preview 301; # reverse one\nset preview 302; # reverse two\n")
    history.chmod(0o600)
    terminal = Terminal(shell, root)
    try:
        terminal.send(b"hel\t\n")
        terminal.until(b"OHLC commands")
        terminal.until(b"Current settings")
        terminal.until(b"ohlc> ")
        terminal.send(b"help ke\t;\n")
        terminal.until(b"OHLC keyboard shortcuts")
        terminal.until(b"Current settings")
        terminal.until(b"ohlc> ")

        cases = [
            (b"set preview 101junk" + b"\x02" * 4 + b"\x0b", 101),
            (b"junkset preview 102\x01" + b"\x06" * 4 + b"\x15\x05", 102),
            (b"set preview rubbish\x17" + b"103", 103),
            (b"set preview 104\x01\x0b\x19", 104),
            (b"set preview 105\x17\x17\x19", 105),
            (b"set preview 106\x01\x1bd\x1bd\x19\x05", 106),
            (b"set preview 107\x01\x0b\x19", 107),
            (b"\x19", 107),  # The cut buffer survives the next prompt.
            (b"xset preview 108x\x1b[H\x1b[3~\x1b[F\x7f", 108),
            (b"xset preview 109\x01\x04\x05\x04", 109),
            (b"set preview 110x\x08", 110),
            (b"set preview 121\x14", 112),
            (b"set garbage 113\x01\x1bf\x1bd preview\x05", 113),
            (b"set preview 114\x1b[1;5D2\x1b[1;5C", 2114),
            (b"set preview 115\x0c", 115),
            (b"set preview 116\x1b\x7f" + b"116", 116),
            (b"set preview 117\x1bb\x1bf", 117),
            (b"set preview 41\x02\x12reverse\x070", 401),
            (b"set preview 51\x02\x10\x0e0", 501),
            (b"set preview 61\x1b[D\x1b[A\x1b[B0", 601),
        ]
        for keys, expected in cases:
            terminal.preview(keys, expected)

        # Search incrementally, go to an older match, then submit it.
        terminal.send(b"\x12reverse")
        terminal.until(b"(reverse-i-search)`reverse': set preview 302;")
        terminal.send(b"\x12\nhelp set;\n")
        terminal.expect_preview(301)

        # A failed refinement preserves the last match; Backspace recovers it.
        terminal.send(b"\x12reverse!")
        terminal.until(b"(failed reverse-i-search)`reverse!':")
        terminal.send(b"\x7f\nhelp set;\n")
        terminal.expect_preview(301)

        # Escape accepts the result without executing it, allowing further editing.
        terminal.send(b"\x12reverse two\x1b")
        terminal.until(b"\r\x1b[2Kohlc> set preview 302; # reverse two")
        terminal.preview(b"\x01\x0bset preview 118", 118)

        terminal.preview(b"discard this\x12reverse\x03set preview 119", 119)
        terminal.send(b"set preview\n")
        terminal.until(b"...> ")
        terminal.send(b"999\x03help set;\n")
        terminal.expect_preview(119)

        terminal.preview(b"set history off; set preview 120", 120)
        terminal.preview(b"set preview 121\x10\x0e\x12", 121)
        terminal.preview(b"set history on; set preview 122", 122)
        terminal.close()
        saved = history.read_bytes()
        assert history.stat().st_mode & 0o777 == 0o600
        assert b"rubbish" not in saved and b"discard this" not in saved
        assert b"set preview 121; help set;" not in saved
        print("editing, incremental search, draft restoration, history privacy and termios: OK")
    finally:
        terminal.cleanup()


def narrow_check(shell, root):
    for columns in (20, 40):
        terminal = Terminal(shell, root, columns=columns)
        try:
            # Long drafts and search terms must not wrap or underflow the viewport.
            terminal.send(b"x" * 300 + b"\x12" + b"missing" * 20 + b"\x07\x01\x0b")
            terminal.preview(b"set preview 123", 123)
            terminal.close()
            for frame in terminal.transcript.split(b"\r\x1b[2K")[1:]:
                if b"\n" in frame:
                    continue  # Command output is separate from line editing.
                visible = re.sub(rb"\x1b\[[0-9;]*[A-Za-z]", b"", frame)
                assert len(visible) < columns, (columns, visible)
        finally:
            terminal.cleanup()
    print("20/40-column scrolling and bounded search prompts: OK")


def disabled_history_check(shell, root):
    history = root / "ohlc-history"
    before = history.read_bytes()
    terminal = Terminal(shell, root, no_history=True)
    try:
        terminal.preview(b"set preview 124\x10\x0e\x12", 124)
        terminal.close()
        assert history.read_bytes() == before
    finally:
        terminal.cleanup()
    print("--no-history leaves existing history untouched: OK")


def main():
    shell = os.path.abspath(sys.argv[1])
    with tempfile.TemporaryDirectory(prefix="ohlc-editor-") as directory:
        root = Path(directory)
        editing_check(shell, root)
        narrow_check(shell, root)
        disabled_history_check(shell, root)


if __name__ == "__main__":
    main()
