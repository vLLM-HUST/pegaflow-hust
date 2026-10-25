"""Verify the curated package, not the original complete dual-node experiment."""

import gzip
import hashlib
import json
import math
import os
from pathlib import Path

ROOT = Path(os.environ["BENCHMARK_COLLECTION_ROOT"]) / "experiments/issue23_handoff"
REPO = ROOT.parents[1]


def read(path):
    if not path.exists() and path.with_suffix(path.suffix + ".gz").exists():
        path = path.with_suffix(path.suffix + ".gz")
    data = (
        gzip.decompress(path.read_bytes())
        if path.suffix == ".gz"
        else path.read_bytes()
    )
    return json.loads(data.decode("utf-8"))


def check_manifest(path, base):
    for line in path.read_text(encoding="utf-8").splitlines():
        expected, name = line.split(None, 1)
        actual = hashlib.sha256((base / name.strip()).read_bytes()).hexdigest()
        if actual != expected:
            raise ValueError("Content hash differs: " + name)


def verify():
    summary = read(ROOT / "evidence/real-results-summary.json")["arms"]
    reference = None
    checked = 0
    for version in ("event-queue-01", "resource-DAG-01"):
        for case in (150, 300, 600):
            directory = ROOT / "evidence" / version / str(case)
            assert read(directory / "audit.json")["passed"] is True
            for arm in ("baseline", "treatment"):
                values = read(directory / (arm + "-requests.json"))
                assert len(values) == 95 and len({r["id"] for r in values}) == 95
                mapping = {r["id"]: r for r in values}
                if reference is None:
                    reference = mapping
                assert set(mapping) == set(reference)
                for key, r in mapping.items():
                    assert len(r["D"]["output_token_ids"]) == 128
                    assert (
                        r["D"]["output_token_ids"]
                        == reference[key]["D"]["output_token_ids"]
                    )
                    assert (
                        r["P"]["output_token_ids"][0] == r["D"]["output_token_ids"][0]
                    )
                    assert (
                        r["P"]["prompt_tokens"] == reference[key]["P"]["prompt_tokens"]
                    )
                q99 = max(r["ttft_seconds"] for r in values)
                assert (
                    abs(
                        q99
                        - summary[f"{version}:{case}:{arm}"]["conventional_TTFT"]["p99"]
                    )
                    < 1e-8
                )
                checked += len(values)
    directory = ROOT / "evidence/detached-calendar-01"
    audit = read(directory / "full-audit.json")
    assert audit["passed"] and audit["strict_initial_cache_equal"]
    for arm in ("B-before", "T-off", "B-after"):
        values = read(directory / (arm + "-requests.json"))
        assert len(values) == 95
        for r in values:
            assert (
                r["D"]["output_token_ids"]
                == reference[r["id"]]["D"]["output_token_ids"]
            )
        assert (
            abs(
                max(r["ttft_seconds"] for r in values)
                - summary[f"detached-calendar-01:300:{arm}"]["conventional_TTFT"]["p99"]
            )
            < 1e-8
        )
        checked += len(values)
    runtime = REPO / "experiments/issue23_strategies/runtime"
    check_manifest(runtime / "source.sha256", runtime)
    check_manifest(ROOT / "MANIFEST.sha256", ROOT)
    screen = read(ROOT / "evidence/handoff-rescreen-20261019.json")
    assert len(screen["cases"]) == 6
    for case in screen["cases"]:
        for arm in case["arms"].values():
            assert (
                arm["requests"] == 95
                and arm["same_routing_and_D_boundary_solver_as_live"]
            )
            assert all(
                math.isfinite(v["TTFT"]) and v["TTFT"] >= 0 for v in arm["per_request"]
            )
    print(
        json.dumps(
            {
                "curated_request_records_checked": checked,
                "six_CPU_cases": True,
                "snapshot_and_package_hashes_match": True,
                "not_a_replacement_for_full_raw_dual_node_audit": True,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    verify()
