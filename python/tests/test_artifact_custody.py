from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[2]
MODULE_PATH = ROOT / "scripts" / "verify_artifact_custody.py"
SPEC = importlib.util.spec_from_file_location("verify_artifact_custody", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
custody = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(custody)


def write(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def policy(*, count: int = 1, formal: list | None = None, exploratory: list | None = None):
    return {
        "schema_version": "pegaflow.artifact-custody-policy/v1",
        "expected_artifact_count": count,
        "formal": formal or [],
        "exploratory": exploratory
        if exploratory is not None
        else [{"glob": "results/**/trace_audit.json", "reason": "history-only"}],
    }


def artifact(commit: str = "a" * 40) -> dict:
    return {"_env": {"git_commit": commit}}


def test_current_repository_inventory_is_complete() -> None:
    report = custody.verify(
        ROOT,
        ROOT / "evidence/custody/artifact-policy.json",
        check_refs=False,
    )

    assert report["status"] == "VERIFIED"
    assert report["counts"] == {"total": 42, "formal": 0, "exploratory": 42}


def test_unclassified_or_count_drift_fails_closed(tmp_path: Path) -> None:
    write(tmp_path / "results/run/trace_audit.json", artifact())
    write(tmp_path / "policy.json", policy(count=2))
    with pytest.raises(custody.CustodyError, match="artifact count changed"):
        custody.verify(tmp_path, tmp_path / "policy.json", check_refs=False)

    write(
        tmp_path / "policy.json",
        policy(count=1, exploratory=[{"glob": "other/**", "reason": "wrong"}]),
    )
    with pytest.raises(custody.CustodyError, match="exactly one"):
        custody.verify(tmp_path, tmp_path / "policy.json", check_refs=False)


def formal_entry() -> dict:
    return {
        "artifact": "results/run/trace_audit.json",
        "pegaflow": {"repository": "repo", "commit": "a" * 40, "ref": "refs/tags/run"},
        "vllm": {"repository": "repo", "commit": "b" * 40, "ref": "refs/tags/core"},
        "vllm_ascend": {
            "repository": "repo",
            "commit": "c" * 40,
            "ref": "refs/tags/ascend",
        },
        "model": {"id": "model", "config_sha256": "d" * 64},
        "environment": {"cann": "9.1", "torch_npu": "2.10", "device_mapping": "0,1"},
        "command": "python scripts/run.py --frozen",
        "raw_logs": [{"path": "results/run/log.txt", "sha256": "0" * 64}],
    }


def test_formal_artifact_requires_logs_command_and_matching_commits(tmp_path: Path) -> None:
    entry = formal_entry()
    write(
        tmp_path / "results/run/trace_audit.json",
        {
            "_env": {
                "git_commit": "a" * 40,
                "runtime_commit_vllm": "b" * 40,
                "runtime_commit_ascend": "c" * 40,
                "reproduce_command": entry["command"],
                "model": "model",
                "model_config_sha256": "d" * 64,
                "cann": "9.1",
                "torch_npu": "2.10",
                "device_mapping": "0,1",
            }
        },
    )
    write(tmp_path / "policy.json", policy(formal=[entry], exploratory=[]))

    with pytest.raises(custody.CustodyError, match="entrypoint is missing"):
        custody.verify(tmp_path, tmp_path / "policy.json", check_refs=False)

    (tmp_path / "scripts").mkdir()
    (tmp_path / "scripts/run.py").write_text("print('ok')\n", encoding="utf-8")
    with pytest.raises(custody.CustodyError, match="raw log is missing or changed"):
        custody.verify(tmp_path, tmp_path / "policy.json", check_refs=False)


def test_formal_artifact_with_complete_local_custody_passes(tmp_path: Path) -> None:
    entry = formal_entry()
    (tmp_path / "scripts").mkdir()
    (tmp_path / "scripts/run.py").write_text("print('ok')\n", encoding="utf-8")
    log = tmp_path / "results/run/log.txt"
    log.parent.mkdir(parents=True)
    log.write_text("raw\n", encoding="utf-8")
    entry["raw_logs"][0]["sha256"] = custody._sha256(log)
    write(
        tmp_path / "results/run/trace_audit.json",
        {
            "_env": {
                "git_commit": "a" * 40,
                "runtime_commit_vllm": "b" * 40,
                "runtime_commit_ascend": "c" * 40,
                "reproduce_command": entry["command"],
                "model": "model",
                "model_config_sha256": "d" * 64,
                "cann": "9.1",
                "torch_npu": "2.10",
                "device_mapping": "0,1",
            }
        },
    )
    write(tmp_path / "policy.json", policy(formal=[entry], exploratory=[]))

    report = custody.verify(tmp_path, tmp_path / "policy.json", check_refs=False)

    assert report["counts"] == {"total": 1, "formal": 1, "exploratory": 0}


def test_formal_artifact_rejects_metadata_drift_and_path_escape(tmp_path: Path) -> None:
    entry = formal_entry()
    entry["raw_logs"] = [{"path": "../outside.log", "sha256": "0" * 64}]
    (tmp_path / "scripts").mkdir()
    (tmp_path / "scripts/run.py").write_text("print('ok')\n", encoding="utf-8")
    write(
        tmp_path / "results/run/trace_audit.json",
        {
            "_env": {
                "git_commit": "a" * 40,
                "runtime_commit_vllm": "b" * 40,
                "runtime_commit_ascend": "c" * 40,
                "reproduce_command": entry["command"],
                "model": "model",
                "model_config_sha256": "d" * 64,
                "cann": "9.1",
                "torch_npu": "2.10",
                "device_mapping": "0,1",
            }
        },
    )
    write(tmp_path / "policy.json", policy(formal=[entry], exploratory=[]))

    with pytest.raises(custody.CustodyError, match="repository-relative"):
        custody.verify(tmp_path, tmp_path / "policy.json", check_refs=False)

    entry["raw_logs"] = [{"path": "results/run/log.txt", "sha256": "0" * 64}]
    entry["model"]["id"] = "different-model"
    write(tmp_path / "policy.json", policy(formal=[entry], exploratory=[]))
    with pytest.raises(custody.CustodyError, match="model identity"):
        custody.verify(tmp_path, tmp_path / "policy.json", check_refs=False)
