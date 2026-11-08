#!/usr/bin/env python3
"""Fail-closed custody verification for PegaFlow serving artifacts."""

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import json
import pathlib
import re
import shlex
import subprocess
import sys
from collections.abc import Sequence
from typing import Any

POLICY_SCHEMA = "pegaflow.artifact-custody-policy/v1"
REPORT_SCHEMA = "pegaflow.artifact-custody-report/v1"
HEX40 = re.compile(r"[0-9a-f]{40}")
SHA256 = re.compile(r"[0-9a-f]{64}")
FORMAL_FIELDS = frozenset(
    {
        "artifact",
        "pegaflow",
        "vllm",
        "vllm_ascend",
        "model",
        "environment",
        "command",
        "raw_logs",
    }
)


class CustodyError(ValueError):
    """Custody policy or artifact is incomplete."""


def _object(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise CustodyError(f"{name} must be an object")
    return value


def _text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise CustodyError(f"{name} must be non-empty text")
    return value


def _load(path: pathlib.Path) -> dict[str, Any]:
    try:
        return _object(json.loads(path.read_text(encoding="utf-8")), str(path))
    except (OSError, json.JSONDecodeError) as exc:
        raise CustodyError(f"cannot read {path}: {exc}") from exc


def _sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _checked_path(root: pathlib.Path, value: Any, name: str) -> pathlib.Path:
    relative = pathlib.PurePosixPath(_text(value, name))
    if relative.is_absolute() or ".." in relative.parts:
        raise CustodyError(f"{name} must be a repository-relative path")
    path = (root / relative).resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise CustodyError(f"{name} escapes the repository") from exc
    return path


def _commit_binding(value: Any, name: str, *, check_refs: bool) -> dict[str, str]:
    binding = _object(value, name)
    if set(binding) != {"repository", "commit", "ref"}:
        raise CustodyError(f"{name} must contain repository, commit and ref")
    repository = _text(binding["repository"], f"{name}.repository")
    commit = _text(binding["commit"], f"{name}.commit")
    ref = _text(binding["ref"], f"{name}.ref")
    if not HEX40.fullmatch(commit):
        raise CustodyError(f"{name}.commit must be lowercase 40-hex")
    if not ref.startswith(("refs/heads/", "refs/tags/")):
        raise CustodyError(f"{name}.ref must be a durable branch or tag")
    if check_refs:
        result = subprocess.run(
            ["git", "ls-remote", repository, ref],
            capture_output=True,
            check=False,
            text=True,
            timeout=60,
        )
        rows = [line.split() for line in result.stdout.splitlines() if line.split()]
        if result.returncode != 0 or not any(row[0] == commit for row in rows):
            raise CustodyError(f"{name}.ref does not resolve to {commit}")
    return {"repository": repository, "commit": commit, "ref": ref}


def _validate_formal(
    root: pathlib.Path,
    entry: dict[str, Any],
    artifact_payload: dict[str, Any],
    *,
    check_refs: bool,
) -> None:
    if set(entry) != FORMAL_FIELDS:
        raise CustodyError(
            f"formal {entry.get('artifact', '<unknown>')} field mismatch; "
            f"expected {sorted(FORMAL_FIELDS)}"
        )
    artifact_name = _text(entry["artifact"], "formal.artifact")
    env = _object(artifact_payload.get("_env"), f"{artifact_name}._env")
    pegaflow = _commit_binding(
        entry["pegaflow"], f"{artifact_name}.pegaflow", check_refs=check_refs
    )
    vllm = _commit_binding(
        entry["vllm"], f"{artifact_name}.vllm", check_refs=check_refs
    )
    ascend = _commit_binding(
        entry["vllm_ascend"],
        f"{artifact_name}.vllm_ascend",
        check_refs=check_refs,
    )
    expected = {
        "git_commit": pegaflow["commit"],
        "runtime_commit_vllm": vllm["commit"],
        "runtime_commit_ascend": ascend["commit"],
    }
    for field, value in expected.items():
        if env.get(field) != value:
            raise CustodyError(f"{artifact_name} {field} does not match formal policy")

    model = _object(entry["model"], f"{artifact_name}.model")
    if set(model) != {"id", "config_sha256"}:
        raise CustodyError(f"{artifact_name}.model must contain id and config_sha256")
    model_id = _text(model["id"], f"{artifact_name}.model.id")
    config_sha256 = _text(
        model["config_sha256"], f"{artifact_name}.model.config_sha256"
    )
    if not SHA256.fullmatch(config_sha256):
        raise CustodyError(f"{artifact_name}.model.config_sha256 is invalid")
    if env.get("model") != model_id or env.get("model_config_sha256") != config_sha256:
        raise CustodyError(
            f"{artifact_name} model identity does not match formal policy"
        )

    environment = _object(entry["environment"], f"{artifact_name}.environment")
    if set(environment) != {"cann", "torch_npu", "device_mapping"}:
        raise CustodyError(
            f"{artifact_name}.environment must contain cann, torch_npu and device_mapping"
        )
    for field in environment:
        value = _text(environment[field], f"{artifact_name}.environment.{field}")
        if env.get(field) != value:
            raise CustodyError(
                f"{artifact_name} environment.{field} does not match artifact _env"
            )

    command = _text(entry["command"], f"{artifact_name}.command")
    if env.get("reproduce_command") != command:
        raise CustodyError(f"{artifact_name} reproduce_command does not match policy")
    try:
        command_parts = shlex.split(command)
    except ValueError as exc:
        raise CustodyError(
            f"{artifact_name} command is not valid shell syntax"
        ) from exc
    if len(command_parts) < 2 or command_parts[0] not in {"python", "python3"}:
        raise CustodyError(
            f"{artifact_name} command must use a checked-in Python entrypoint"
        )
    command_path = _checked_path(
        root, command_parts[1], f"{artifact_name}.command entrypoint"
    )
    if not command_path.is_file():
        raise CustodyError(f"{artifact_name} command entrypoint is missing")

    logs = entry["raw_logs"]
    if not isinstance(logs, list) or not logs:
        raise CustodyError(f"{artifact_name}.raw_logs must be non-empty")
    for index, log in enumerate(logs):
        item = _object(log, f"{artifact_name}.raw_logs[{index}]")
        if set(item) != {"path", "sha256"}:
            raise CustodyError(f"{artifact_name}.raw_logs[{index}] field mismatch")
        log_path = _checked_path(
            root,
            item["path"],
            f"{artifact_name}.raw_logs[{index}].path",
        )
        expected_hash = _text(
            item["sha256"], f"{artifact_name}.raw_logs[{index}].sha256"
        )
        if not SHA256.fullmatch(expected_hash):
            raise CustodyError(f"{artifact_name}.raw_logs[{index}].sha256 is invalid")
        if not log_path.is_file() or _sha256(log_path) != expected_hash:
            raise CustodyError(
                f"{artifact_name} raw log is missing or changed: {log_path}"
            )


def verify(
    root: pathlib.Path, policy_path: pathlib.Path, *, check_refs: bool = True
) -> dict[str, Any]:
    root = root.resolve()
    policy = _load(policy_path)
    if policy.get("schema_version") != POLICY_SCHEMA:
        raise CustodyError("unsupported custody policy schema")
    expected_count = policy.get("expected_artifact_count")
    if not isinstance(expected_count, int) or expected_count < 0:
        raise CustodyError("expected_artifact_count must be non-negative")
    formal_entries = policy.get("formal")
    exploratory_rules = policy.get("exploratory")
    if not isinstance(formal_entries, list) or not isinstance(exploratory_rules, list):
        raise CustodyError("formal and exploratory must be arrays")

    formal_by_path: dict[str, dict[str, Any]] = {}
    for entry in formal_entries:
        item = _object(entry, "formal entry")
        artifact = _text(item.get("artifact"), "formal.artifact")
        if artifact in formal_by_path:
            raise CustodyError(f"duplicate formal artifact: {artifact}")
        formal_by_path[artifact] = item

    rules: list[tuple[str, str]] = []
    for rule in exploratory_rules:
        item = _object(rule, "exploratory rule")
        if set(item) != {"glob", "reason"}:
            raise CustodyError("exploratory rule must contain glob and reason")
        rules.append(
            (
                _text(item["glob"], "exploratory.glob"),
                _text(item["reason"], "exploratory.reason"),
            )
        )

    artifact_paths = sorted(root.glob("results/**/trace_audit.json"))
    if len(artifact_paths) != expected_count:
        raise CustodyError(
            f"artifact count changed: expected {expected_count}, found {len(artifact_paths)}"
        )
    formal: list[str] = []
    exploratory: list[dict[str, str]] = []
    for artifact_path in artifact_paths:
        relative = artifact_path.relative_to(root).as_posix()
        payload = _load(artifact_path)
        if relative in formal_by_path:
            _validate_formal(
                root,
                formal_by_path[relative],
                payload,
                check_refs=check_refs,
            )
            formal.append(relative)
            continue
        matched = [
            (pattern, reason)
            for pattern, reason in rules
            if fnmatch.fnmatch(relative, pattern)
        ]
        if len(matched) != 1:
            raise CustodyError(
                f"{relative} must match exactly one exploratory rule, got {len(matched)}"
            )
        env = _object(payload.get("_env"), f"{relative}._env")
        _text(env.get("git_commit"), f"{relative}._env.git_commit")
        exploratory.append({"artifact": relative, "reason": matched[0][1]})

    missing_formal = sorted(set(formal_by_path).difference(formal))
    if missing_formal:
        raise CustodyError(
            f"formal policy references missing artifacts: {missing_formal}"
        )
    return {
        "schema_version": REPORT_SCHEMA,
        "status": "VERIFIED",
        "formal": formal,
        "exploratory": exploratory,
        "counts": {
            "total": len(artifact_paths),
            "formal": len(formal),
            "exploratory": len(exploratory),
        },
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=pathlib.Path, default=pathlib.Path.cwd())
    parser.add_argument(
        "--policy",
        type=pathlib.Path,
        default=pathlib.Path("evidence/custody/artifact-policy.json"),
    )
    parser.add_argument("--offline", action="store_true")
    args = parser.parse_args(argv)
    policy = args.policy if args.policy.is_absolute() else args.root / args.policy
    try:
        report = verify(args.root, policy, check_refs=not args.offline)
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0
    except CustodyError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
