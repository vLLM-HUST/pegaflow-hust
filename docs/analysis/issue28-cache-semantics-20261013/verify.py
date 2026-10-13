"""Verify the frozen semantic diagnosis packet using the Python standard library."""

from __future__ import annotations

import ast
import hashlib
import json
import zipfile
from pathlib import Path


def verify(root: Path) -> dict:
    for line in (root / "SHA256SUMS").read_text(encoding="utf-8").splitlines():
        expected, name = line.split("  ", 1)
        assert hashlib.sha256((root / name).read_bytes()).hexdigest() == expected, name

    summary = json.loads((root / "summary.json").read_text(encoding="utf-8"))
    replay = json.loads(
        (root / "decode-semantic-replay.json").read_text(encoding="utf-8")
    )
    phases = {r["phase"]: r for r in summary["requests"]}
    assert len(phases) == 4
    assert phases["cold1"]["token_ids"] == phases["cold2"]["token_ids"]
    assert phases["cold1"]["token_ids"] != phases["reuse"]["token_ids"]
    assert phases["reuse"]["prefix_hits"] == 2048
    assert all(
        r["external_hits"] == 0 and len(r["token_ids"]) == 32 for r in phases.values()
    )
    assert summary["input_prefix_with_generated_token_exact"]
    assert len(replay["restored_states"]) == 120
    assert all(r["exact"] for r in replay["restored_states"])
    for rank in replay["ranks"].values():
        assert rank["cold_first_forward_tokens"] == 2048
        assert rank["producer_first_forward_tokens"] == 2047
        assert all(rank["input-prefix-" + k]["exact"] for k in ("qkv", "a", "b"))
        assert all(rank["input-token2048-" + k]["exact"] for k in ("qkv", "a", "b"))
        assert all(
            rank["operand-token2048-" + k]["exact"]
            for k in ("q", "k", "v", "g", "beta")
        )
        assert rank["replayed-decode-ssm"]["exact"]
        assert rank["replayed-decode-output"]["exact"]
        assert rank["decode-output-vs-float64-rounded-bf16"]["exact"]
        assert rank["decode-vs-float64-same-initial-state"]["relative_rms"] < 5e-8
        assert rank["float64-chunk-vs-float64-recurrence-state"]["max_abs"] < 2e-13
        assert rank["float64-chunk-vs-float64-recurrence-output"]["max_abs"] < 2e-14
        native_error = rank["prefill2048-vs-float64"]["rms"]
        residual = rank["native-prefill-vs-cpu-bf16-state-emulation"]["rms"]
        assert residual < native_error * 0.02

    with zipfile.ZipFile(root / "diagnostic-packet.zip") as archive:
        assert archive.testzip() is None
        assert (
            hashlib.sha256(archive.read("cases.json")).hexdigest()
            == summary["input_cases_sha256"]
        )
        audit = json.loads(archive.read("prefix-identity-audit.json"))
        assert audit["summary"] == summary["prefix_identity"]
        assert len(audit["records"]) == 3160 and not audit["violations"]
        for record in audit["records"]:
            assert record["matches"]
            assert all(
                m["same_prefix"] and m["publisher_boundary_within_prompt"]
                for m in record["matches"]
            )
        for name in archive.namelist():
            if name.endswith(".py"):
                ast.parse(archive.read(name), filename=name)
    return {
        "requests": 4,
        "exact_state_restorations": 120,
        "valid_loaded_plane_prefixes": 3160,
        "strict_output_gate": "failed",
        "semantic_acceptance_scope": summary["acceptance_scope"],
    }


if __name__ == "__main__":
    print(json.dumps(verify(Path(__file__).resolve().parent), indent=2))
