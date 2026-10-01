from __future__ import annotations

import json
from pathlib import Path

import tomllib

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "pegaflow/ecpa_bundle/vllm-hust-extension-v0.3.json"


def test_runtime_distribution_owns_ecpa_bundle_and_plugin_target() -> None:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    entry_points = project["entry-points"]
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))

    assert entry_points["vllm_hust.extension_bundles"][manifest["extension_id"]] == (
        "pegaflow.ecpa_bundle"
    )
    assert entry_points["vllm.general_plugins"]["pegaflow"] == ("pegaflow.vllm_plugin:register")
    assert (ROOT / "pegaflow/vllm_plugin.py").is_file()
    assert manifest["schema_version"] == "0.3-experimental"
    assert manifest["extension_version"] == project["version"]


def test_external_operator_boundary_and_exclusive_connector_claim() -> None:
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))

    assert manifest["lifecycle_owner"] == "external_operator"
    assert manifest["requires_services"][0]["optional"] is False
    assert manifest["resource_claims"] == [
        {
            "resource": "vllm.kv-connector.primary",
            "scope": "vllm-process",
            "mode": "exclusive",
        }
    ]
