"""Explicit cache-state and allocation contracts for the separate B/T/B job."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import platform
import re
import subprocess

from pegaflow.placement_executor import NodeInventory


def canonical_inventory(payload: dict) -> dict:
    """Retain ownership topology/accounting, excluding ephemeral identity only."""
    NodeInventory.parse(1, payload)
    blocks = payload["cache"]["blocks"]
    owners = {item["id"]: [] for item in payload["cache"]["allocations"]}
    logical = []
    for block in blocks:
        key = (block["key"]["namespace"], bytes(block["key"]["hash"]).hex())
        logical.append((key, block["slots"], block["payload_bytes"], block["protected"]))
        for identity in block["allocation_ids"]:
            owners[identity].append(key)
    allocations = sorted(
        (item["charged_bytes"], item["protected"], tuple(sorted(owners[item["id"]])))
        for item in payload["cache"]["allocations"]
    )
    return {
        "namespace": payload["namespace"],
        "group_slots": payload["group_slots"],
        "capacity": payload["allocator_capacity_bytes"],
        "used": payload["allocator_used_bytes"],
        "visible": payload["visible_allocation_bytes"],
        "nonvisible": payload["nonvisible_used_bytes"],
        "largest_free": payload["largest_free_allocation_bytes"],
        "blocks": sorted(logical),
        "allocations_with_multiplicity": allocations,
    }


def state_digest(payloads: list[dict], *, empty: bool = False) -> str:
    if len(payloads) != 4:
        raise ValueError("Exactly four instance inventories required")
    values = [canonical_inventory(payload) for payload in payloads]
    if empty and any(value["used"] or value["blocks"] or value["allocations_with_multiplicity"]
                     for value in values):
        raise ValueError("Fresh pre-warmup cache has live/hidden allocations")
    return hashlib.sha256(json.dumps(values, sort_keys=True).encode()).hexdigest()


def allocation_identity() -> dict:
    """Read actual assigned devices/container; no workload or occupancy action."""
    mapping = subprocess.run(["npu-smi", "info", "-m"], check=True,
                             capture_output=True, text=True).stdout
    state = subprocess.run(["npu-smi", "info"], check=True,
                           capture_output=True, text=True).stdout
    devices = sorted([path.name, os.stat(path).st_rdev] for path in Path("/dev").glob("davinci[0-9]*"))
    pci = sorted(set(re.findall(r"[0-9a-fA-F]{4}:[0-9a-fA-F]{2}:[0-9a-fA-F]{2}\.[0-7]", state)))
    if len(devices) != 2 or len(pci) != 2:
        raise ValueError("Could not establish both physical device/PCI identities")
    # Field22 is starttime. Split after the final process-name parenthesis.
    init_start = Path("/proc/1/stat").read_text().rsplit(")", 1)[1].split()[19]
    return {"mapping": mapping, "devices": devices, "pci": pci,
            "container": platform.node(), "init_start_ticks": init_start,
            "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
            "gid5": Path("/sys/class/infiniband/mlx5_2/ports/1/gids/5").read_text().strip()}


def require_same(expected, actual, label: str) -> None:
    if expected != actual:
        raise ValueError(f"Confirmation {label} changed; do not splice across states")
