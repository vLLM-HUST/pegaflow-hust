"""Keep archived execution source immutable instead of lint-rewriting history."""

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path


def test_original_runtime_source_hashes():
    root = Path(os.environ["BENCHMARK_COLLECTION_ROOT"]) / "experiments/issue23_strategies/runtime"
    rows = {}
    for line in (root / "source.sha256").read_text().splitlines():
        sha, name = line.split(maxsplit=1)
        assert name not in rows
        rows[name] = sha
        assert hashlib.sha256((root / name).read_bytes()).hexdigest() == sha, name
    confirmation = json.loads((root / "confirmation-sources.json").read_text())
    for name, sha in confirmation.items():
        assert rows[name] == sha
    repo = Path(__file__).resolve().parents[2]
    prefix = "experiments/issue23_strategies/runtime/"
    tracked = subprocess.check_output(
        [
            "git",
            "-C",
            str(repo),
            "ls-files",
            "--",
            prefix + "code/",
            prefix + "overlay/",
        ],
        text=True,
    ).splitlines()
    assert tracked
    assert all(path.removeprefix(prefix) in rows for path in tracked)
    assert all(
        hashlib.sha256((repo / path).read_bytes()).hexdigest() == rows[path.removeprefix(prefix)]
        for path in tracked
    )


def test_original_handoff_verifier_and_measurements():
    repo = Path(__file__).resolve().parents[2]
    subprocess.run(
        [sys.executable, str(repo / "experiments/issue23_handoff/verify_handoff.py")],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    )
