"""Narrow tracked-diff high-signal secret scan, not a repository-wide security audit."""

from __future__ import annotations

import re
import subprocess


def main():
    text = subprocess.run(
        ["git", "diff", "--cached", "--unified=0"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    patterns = [
        r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----",
        r"\b(?:ghp_|github_pat_|sk-proj-)[A-Za-z0-9_-]{16,}",
    ]
    # Inspect added lines only; never print a matched value.
    added = "\n".join(
        line[1:]
        for line in text.splitlines()
        if line.startswith("+") and not line.startswith("+++")
    )
    hits = sum(len(re.findall(pattern, added)) for pattern in patterns)
    print(f"High-signal added-line secret matches: {hits}")
    if hits:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
