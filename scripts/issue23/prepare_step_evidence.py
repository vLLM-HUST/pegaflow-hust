"""Normalize measured calibration and validation traces for CPU simulation.

Only token counts and chained prefix identities are exported from prompt data.
Calibration costs and validation observations are written to separate files.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import statistics
from pathlib import Path

from step_simulator import features, write_json


def read_rows(path):
    return [
        json.loads(line)
        for line in Path(path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def write_rows(path, rows):
    raw = "".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows).encode()
    Path(path).write_bytes(gzip.compress(raw, mtime=0))


def prefix_ids(tokens, size=128):
    parent = b""
    result = []
    for start in range(0, len(tokens) - size + 1, size):
        raw = b"".join(int(token).to_bytes(4, "little") for token in tokens[start : start + size])
        parent = hashlib.sha256(parent + raw).digest()
        result.append(parent.hex())
    return result


def extract(root, source_requests):
    cases = {r["case"]: r for r in read_rows(root / "scheduling-cases.jsonl")}
    workloads = {r["case"]: r for r in read_rows(root / "workloads.jsonl")}
    events = []
    for path in (root / "scheduler-trace").glob("*.jsonl"):
        events.extend(read_rows(path))
    config = next(e for e in events if e["event"] == "scheduler_config")
    schedules = sorted((e for e in events if e["event"] == "schedule"), key=lambda r: r["step"])
    updates = {e["step"]: e for e in events if e["event"] == "scheduler_update"}
    result = {}
    delays = []
    for name, case in cases.items():
        workload = workloads[name]
        ids = {r["request_id"]: f"r{index:03d}" for index, r in enumerate(workload["requests"])}

        def client_id(raw, ids=ids):
            if raw in ids:
                return raw
            base = raw.rsplit("-", 1)[0]
            if base in ids:
                return base
            if base.endswith("-0") and base[:-2] in ids:
                return base[:-2]
            return None

        specs = []
        observed = []
        domains = {r["cache_salt"] for r in workload["requests"]}
        for index, row in enumerate(workload["requests"]):
            raw = case["records"][row["request_id"]]
            src = source_requests[row["source_id"]]
            specs.append(
                {
                    "id": ids[row["request_id"]],
                    "arrival_s": raw["submitted_s"],
                    "prompt_tokens": row["prompt_tokens"],
                    "output_tokens": row["output_tokens"],
                    "prefix_blocks": prefix_ids(src["prompt_token_ids"]),
                    "cache_domain": "shared" if len(domains) == 1 else f"private-{index}",
                    "task": src["task"],
                }
            )
            observed.append(
                {
                    "id": ids[row["request_id"]],
                    "arrival_s": raw["submitted_s"],
                    "first_s": raw["first_s"],
                    "finish_s": raw["finish_s"],
                    "cached_tokens": raw["cached_tokens"],
                }
            )
        frames = []
        trajectory = []
        first_outputs = {}
        previous = None
        for step in schedules:
            assigned = step["scheduled_tokens"]
            if not assigned or not all(client_id(rid) is not None for rid in assigned):
                continue
            update = updates[step["step"]]
            assignments = []
            for rid, count in assigned.items():
                before = step["requests_before"][rid]
                after = step["requests_after"][rid]
                computed = max(before["computed_tokens"], after["computed_tokens"] - count)
                assignments.append((computed, count, before["prompt_tokens"]))
            frame = features(assignments)
            frame.update(
                case=name,
                duration_s=(update["epoch_ns"] - step["epoch_ns"] + step["schedule_wall_ns"]) / 1e9,
                dispatch_gap_s=None,
            )
            if previous is not None and previous["requests"]:
                gap = (step["epoch_ns"] - step["schedule_wall_ns"] - previous["epoch_ns"]) / 1e9
                if 0 <= gap < 0.25:
                    frame["dispatch_gap_s"] = gap
            previous = update
            frames.append(frame)
            trajectory.append(
                {
                    "at_s": (step["epoch_ns"] - workload["epoch_ns"]) / 1e9,
                    "waiting": len(step["waiting_after"]),
                    "waiting_before": len(step["waiting_before"]),
                    "running": len(step["running_after"]),
                    "active_KV_blocks": step["total_blocks"] - step["free_blocks"] - 1,
                    "scheduled": {ids[client_id(key)]: value for key, value in assigned.items()},
                    **features(assignments),
                }
            )
            for rid, row in update["requests"].items():
                external = client_id(rid)
                if external is not None and row["output_tokens"] > 0:
                    first_outputs.setdefault(external, update["epoch_ns"])
        if not name.startswith("warmup"):
            for rid, timestamp in first_outputs.items():
                delays.append(
                    case["records"][rid]["first_s"] - (timestamp - workload["epoch_ns"]) / 1e9
                )
        result[name] = {
            "workload": {
                "schema": "pegaflow-step-workload/v1",
                "engine": {
                    "model": "Qwen2.5-7B-Instruct",
                    "dtype": "bfloat16",
                    "hardware": "Ascend 910B2",
                    "vllm": "0.23.0",
                    "vllm_ascend": "0.23.0",
                },
                "config": {
                    "token_budget": config["max_num_batched_tokens"],
                    "max_sequences": config["max_num_seqs"],
                    "total_blocks": config["num_gpu_blocks"],
                    "block_size": config["block_size"],
                    "enable_prefix_caching": True,
                    "policy": "fcfs",
                },
                "requests": specs,
            },
            "observed": {
                "schema": "pegaflow-step-observation/v1",
                "scope": "Measured input arrivals and outcomes; excluded from cost fitting for validation cases",
                "requests": observed,
                "trajectory": trajectory,
                "peak_active_KV_blocks": max(r["active_KV_blocks"] for r in trajectory),
                "peak_running": max(r["running"] for r in trajectory),
                "peak_waiting": max(r["waiting_before"] for r in trajectory),
            },
            "frames": frames,
        }
    return result, delays


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--calibration-root", type=Path, required=True)
    parser.add_argument("--validation-private-root", type=Path, required=True)
    parser.add_argument("--validation-shared-root", type=Path, required=True)
    parser.add_argument("--requests", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    source = {r["id"]: r for r in read_rows(args.requests)}
    train, delays = extract(args.calibration_root, source)
    private, _ = extract(args.validation_private_root, source)
    shared, _ = extract(args.validation_shared_root, source)
    args.output.mkdir(parents=True, exist_ok=True)
    frames = []
    tasks = set()
    for name in ("train-32-private", "train-32-shared"):
        case = train[name]
        frames.extend(case["frames"])
        tasks.update(r["task"] for r in case["workload"]["requests"])
        write_json(args.output / f"{name}-workload.json.gz", case["workload"])
        write_json(args.output / f"{name}-observed.json.gz", case["observed"])
    write_rows(args.output / "calibration-steps.jsonl.gz", frames)
    metadata = {
        "model": "Qwen2.5-7B-Instruct",
        "dtype": "bfloat16",
        "hardware": "Ascend 910B2",
        "vllm": "0.23.0",
        "vllm_ascend": "0.23.0",
        "token_budget": 2048,
        "max_sequences": 8,
        "max_model_tokens": 32768,
        "KV_GiB": 16,
        "graph_mode": True,
        "training_tasks": sorted(tasks),
        "training_cases": ["train-32-private", "train-32-shared"],
        "training_input_count": 32,
        "response_delay_s": max(0.0, statistics.median(delays)),
        "trajectory_repository": "openguardrails/terminal-bench-2.1-deepseek-v4-flash-trajectories",
        "trajectory_revision": "3f5b93ff1c5ea84b2c94cf479d0c942227191b4a",  # Public Git revision; pragma: allowlist secret
        "source_pegaflow_commit": "4bfa9351273efc710bceb296ae6f8cbdc31584e6",  # Public Git revision; pragma: allowlist secret
        "scope": "Single-engine diagnostic calibration; derived prefix identities and token counts",
    }
    write_json(args.output / "calibration-metadata.json", metadata)
    for label, case in [
        ("validation-95-private", private["full-95-fixed-arrivals"]),
        ("validation-95-shared", shared["full-95-shared-prefix"]),
    ]:
        if tasks & {r["task"] for r in case["workload"]["requests"]}:
            raise ValueError("Calibration and validation tasks overlap")
        write_json(args.output / f"{label}-workload.json.gz", case["workload"])
        write_json(args.output / f"{label}-observed.json.gz", case["observed"])
    print(
        json.dumps(
            {
                "training_steps": len(frames),
                "response_delay_s": metadata["response_delay_s"],
            }
        )
    )


if __name__ == "__main__":
    main()
