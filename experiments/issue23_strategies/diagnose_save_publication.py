"""Minimal CPU counterexample to publishing each allocation during one save.

The staged variant is a small source-contract calculation, not a native run or
an online policy. All requested new blocks fit in the pool after old eviction.
"""

import argparse
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import simulate_continuous_batch  # noqa: F401
from pegaflow.joint_policy import Demand, Layout, Unit
from pegaflow.resource_cache_profile import CacheForecast


def diagnose(repo):
    size = 4096
    old_keys = [f"old-{i}".encode() for i in range(10)]
    keys = tuple(f"new-{i}".encode() for i in range(6))
    units = tuple(
        Unit(str(i), frozenset((k,)), size, ((k, size),))
        for i, k in enumerate(old_keys)
    )
    layout = Layout((units, (), (), ()), (12 * size,) * 4, (0,) * 4)
    demand = Demand("new-save", "session", 6 * 128 + 1, keys, block_tokens=128)
    model = CacheForecast(
        layout,
        {demand.key: demand},
        {},
        SimpleNamespace(bytes_per_token=size // 128),
        512,
    )
    model.save(0, demand)
    # Allocate privately first, as offload.rs holds pages/RawBlocks until GPU
    # copy and only then sends RawSaveBatch to the insert worker.
    published = list(old_keys)
    staging = []
    for key in keys:
        if len(published) + len(staging) >= 12:
            published = published[512:]
        assert len(published) + len(staging) < 12
        staging.append(key)
    published.extend(staging)
    expected = sum(k in published for k in keys)
    actual = model.layout.prefix(0, keys)
    assert actual == 0 and expected == 6
    paths = [
        "pegaflow-core/src/offload.rs",
        "pegaflow-core/src/storage/mod.rs",
        "experiments/issue23_strategies/runtime/overlay/pegaflow/resource_cache_profile.py",
    ]
    return {
        "source_contract_diagnostic_only": True,
        "native_binary_executed": False,
        "all_new_blocks_fit": True,
        "pool_blocks": 12,
        "initial_old_blocks": 10,
        "new_blocks": 6,
        "reclaim_batch_unchanged": 512,
        "current_model_usable_new_prefix": actual,
        "current_model_retained_new_blocks": sum(
            k in model.layout.keys(0) for k in keys
        ),
        "staged_source_contract_new_prefix": expected,
        "mechanism": "Current model inserts new save blocks into eviction candidates before remaining allocations finish; native source owns unpublished allocation references until copy/insert.",
        "source_sha256": {
            n: hashlib.sha256((repo / n).read_bytes()).hexdigest() for n in paths
        },
        "scope_limit": "Not yet mapped to all full-run cache collapses or the installed stable native wheel. Do not retroactively relabel old results or change capacity/costs.",
    }


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    a = p.parse_args()
    result = diagnose(a.repo)
    with a.output.open("x", encoding="utf-8") as f:
        json.dump(result, f, indent=2)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
