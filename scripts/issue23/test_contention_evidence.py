"""Re-signed evidence must still prove complete checkpoint reuse."""

import gzip
import hashlib
import json
import os
import shutil
from pathlib import Path

import pytest
from common import read_json
from verify_contention import compressed, verify


def test_rehashed_wrong_checkpoint_is_rejected(tmp_path):
    source = Path(os.environ["BENCHMARK_EVIDENCE_ROOT"]) / "results/issue23/20261015-contention"
    root = tmp_path / "evidence"
    shutil.copytree(source, root)
    arm = compressed(root / "cohort-0000/rdma-burst.json.gz")
    event = next(e for e in arm["observed.jsonl"] if e["event"] == "bundle_completed")
    telemetry = compressed(root / "telemetry.json.gz")
    row = next(
        r
        for r in telemetry["first_foreground_work"]
        if r["request_id"] == event["raw_request_id"] and r["tp_rank"] == 0
    )
    row["computed_tokens"] = 0
    blob = gzip.compress(json.dumps(telemetry).encode(), mtime=0)
    (root / "telemetry.json.gz").write_bytes(blob)
    manifest = read_json(root / "manifest.json")
    manifest["telemetry.json.gz"] = {
        "bytes": len(blob),
        "sha256": hashlib.sha256(blob).hexdigest(),
    }
    (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError, match="Checkpoint was not reused"):
        verify(root)
