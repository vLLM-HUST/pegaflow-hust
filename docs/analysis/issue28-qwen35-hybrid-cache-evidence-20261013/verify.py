"""Recompute archived output comparisons and verify the public evidence bundle."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import defaultdict
from pathlib import Path


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()


def verify(root: Path, source_root: Path) -> dict:
    repo = source_root
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    for name, expected in manifest["files_sha256"].items():
        if digest(root / name) != expected:
            raise ValueError(f"Evidence digest mismatch: {name}")
    validation = json.loads(
        (
            root.parent / "issue28-qwen35-hybrid-cache-validation-20261013.json"
        ).read_text(encoding="utf-8")
    )
    for name, expected in validation["source_sha256"].items():
        if digest(repo / name) != expected:
            raise ValueError(f"Implementation digest mismatch: {name}")

    cases = json.loads((root / "cases.json").read_text(encoding="utf-8"))
    case_ids = {case["id"] for case in cases}
    outputs = json.loads((root / "outputs.json").read_text(encoding="utf-8"))["records"]
    baselines = defaultdict(list)
    for row in outputs:
        if row["case_id"] not in case_ids or len(row["token_ids"]) != 32:
            raise ValueError(f"Invalid output record: {row['request_id']}")
        if row["phase"] == "baseline":
            baselines[row["case_id"]].append(row["token_ids"])
    if set(baselines) != case_ids:
        raise ValueError("Missing baseline case")
    for case, ids in baselines.items():
        if len(ids) != 2 or ids[0] != ids[1]:
            raise ValueError(f"Unstable baseline: {case}")

    phase_counts = defaultdict(lambda: {"passed": 0, "total": 0, "failed_cases": []})
    for row in outputs:
        equal = row["token_ids"] == baselines[row["case_id"]][0]
        if row["raw_reported_equal"] is not None and equal != row["raw_reported_equal"]:
            raise ValueError(f"Archived comparison disagrees: {row['request_id']}")
        if row["role"] != "target":
            continue
        count = phase_counts[row["phase"]]
        count["total"] += 1
        count["passed"] += int(equal)
        if not equal:
            count["failed_cases"].append(
                {"case_id": row["case_id"], "tag": row.get("tag")}
            )

    for phase in (
        "initial_isolated",
        "updated_original",
        "updated_reverse",
        "updated_extra",
    ):
        expected = validation["acceptance"][phase]
        if any(
            phase_counts[phase][key] != expected[key] for key in ("passed", "total")
        ):
            raise ValueError(f"Acceptance summary disagrees: {phase}")

    controls = []
    for phase, target_tag, native_tag in (
        ("native_history", "decode2k", "decode2k-native"),
        ("native_prefill", "split4k", "split4k-native"),
        ("native_prefill", "coherent4k", "coherent4k-native"),
    ):
        target = next(
            row
            for row in outputs
            if row["phase"] == phase
            and row.get("tag") == target_tag
            and row["role"] == "target"
        )
        native = next(
            row
            for row in outputs
            if row["phase"] == phase and row.get("tag") == native_tag
        )
        controls.append(
            {
                "case_id": target["case_id"],
                "producer": target_tag,
                "all_32_native_equal_to_cross_node": native["token_ids"]
                == target["token_ids"],
                "equals_baseline": native["token_ids"]
                == baselines[native["case_id"]][0],
                "native_first_forward": native["first_forward"],
            }
        )

    publications = json.loads((root / "publications.json").read_text(encoding="utf-8"))
    byte_counts = {
        "records": len(publications["records"]),
        "matching_source": 0,
        "matching_prior_local_only": 0,
        "unresolved": 0,
    }
    tables = publications["index_tables"]
    for request, rank, layer, slot, sha, hashes, source, local in publications[
        "records"
    ]:
        tables["requests"][request]
        tables["layers"][layer]
        if (
            rank not in (0, 1)
            or slot < 0
            or len(tables["sha256"][sha]) != 64
            or not hashes
        ):
            raise ValueError("Invalid checksum record")
        for index in hashes:
            if len(tables["boundary_hashes"][index]) != 64:
                raise ValueError("Invalid boundary hash")
        for index in source + local:
            tables["requests"][index]
        key = (
            "matching_source"
            if source
            else "matching_prior_local_only"
            if local
            else "unresolved"
        )
        byte_counts[key] += 1
    if any(value != publications["summary"][key] for key, value in byte_counts.items()):
        raise ValueError("Checksum classification summary disagrees")

    return {
        "evidence_integrity": "passed",
        "model_output_gate": "failed"
        if any(
            phase_counts[phase]["passed"] != phase_counts[phase]["total"]
            for phase in ("updated_original", "updated_reverse", "updated_extra")
        )
        else "passed",
        "baseline_repeats": 18,
        "target_comparisons": dict(phase_counts),
        "native_controls": controls,
        "checksum_classifications": byte_counts,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--require-model-correctness", action="store_true")
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--source-root", type=Path, required=True)
    arguments = parser.parse_args()
    result = verify(arguments.root, arguments.source_root)
    print(json.dumps(result, indent=2))
    if arguments.require_model_correctness and result["model_output_gate"] != "passed":
        raise SystemExit(1)
