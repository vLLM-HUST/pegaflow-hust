"""Index the published numerical evidence and analysis artifacts."""

import argparse
import hashlib
from pathlib import Path

from step_simulator import write_json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    root = parser.parse_args().root
    members = []
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.name == "manifest.json" or "__pycache__" in path.parts:
            continue
        relative = path.relative_to(root).as_posix()
        if "prediction" in path.name:
            kind = "simulation"
        elif "observed" in path.name or path.name == "calibration-steps.jsonl.gz":
            kind = "normalized_measurement"
        elif "workload" in path.name:
            kind = "prediction_input"
        elif relative.startswith("provenance/"):
            kind = "provenance"
        else:
            kind = "analysis"
        members.append(
            {
                "path": relative,
                "kind": kind,
                "bytes": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        )
    write_json(
        root / "manifest.json",
        {
            "schema": "pegaflow-step-simulator-evidence/v1",
            "scope": "Single-engine development calibration and simulation; classified numerical artifacts",
            "members": members,
        },
    )


if __name__ == "__main__":
    main()
