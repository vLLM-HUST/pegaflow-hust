"""Freeze the same eight benchmark requests and runtime budget for all four arms."""

import argparse
import copy
from collections import Counter
from pathlib import Path

from common import distribution, read_json, sha256, write_json

ARMS = ("rdma-burst", "local_copy-burst", "rdma-smooth", "local_copy-smooth")


def prepare(source, root, output_tokens=64, rounds=1):
    if output_tokens <= 0 or rounds <= 0:
        raise ValueError("Output budget and round count must be positive")
    target = root / "prepared"
    cases = []
    for cohort, split in enumerate(("smoke", "frontier-smoke")):
        original = read_json(source / f"{split}.json")[:4]
        if len(original) != 4:
            raise ValueError("Each benchmark must supply one complete four-request cohort")
        for slot, item in enumerate(original):
            case = copy.deepcopy(item)
            if not 4097 <= case["input_tokens"] <= 8192 - output_tokens:
                raise ValueError("Prepared input is outside the frozen context budget")
            case["smooth_slot"] = slot
            case["planned_offset_seconds"] = cohort * 8 + slot * 0.02
            case["payload"].update(
                model="hybrid-qwen35",
                max_tokens=output_tokens,
                ignore_eos=True,
                return_token_ids=True,
                cache_salt="issue23-frozen-" + case["request_id"],
            )
            cases.append(case)
    if len({case["request_id"] for case in cases}) != len(cases):
        raise ValueError("Benchmark request identities must be distinct")
    target.mkdir(parents=True, exist_ok=False)
    write_json(target / "smoke.json", cases)
    previous = read_json(source / "contract.json")
    contract = {
        **previous,
        "protocol_id": "issue23-hybrid-four-arm-v2",
        "output_tokens": output_tokens,
        "tensor_parallel_size": 2,
        "max_model_len": 8192,
        "max_input_tokens": 8192 - output_tokens,
        "max_num_seqs": 8,
        "max_num_batched_tokens": 4096,
        "device_cache_bytes_per_card": 4 * 1024**3,
        "host_pool_bytes_per_node": 4 * 1024**3,
        "expected_storage_groups": [0, 1, 2, 3],
        "engine_topology": "one engine owns both physical TP ranks",
        "scheduler_policy": "sched_other",
        "execution_mode": "eager_sync",
        "fixed_source_snapshot": True,
        "output_policy": "observe_numerical_differences",
        "source_prepared_sha256": {
            split: sha256(source / f"{split}.json") for split in ("smoke", "frontier-smoke")
        },
        "split_summary": {
            "smoke": {
                "requests": len(cases),
                "cohorts": 2,
                "datasets": dict(Counter(case["dataset"] for case in cases)),
                "input_tokens": distribution([case["input_tokens"] for case in cases]),
                "sha256": sha256(target / "smoke.json"),
            }
        },
        "round_arm_order": [list(ARMS[index % 4 :] + ARMS[: index % 4]) for index in range(rounds)],
        "hardware_qualification": "pending",
        "measurement_scope": "router P/D end-to-end with frozen cache transport bundles",
    }
    write_json(target / "contract.json", contract)
    return contract


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-prepared", type=Path, required=True)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--output-tokens", type=int, default=64)
    parser.add_argument("--rounds", type=int, default=1)
    args = parser.parse_args()
    prepare(args.source_prepared, args.root, args.output_tokens, args.rounds)
