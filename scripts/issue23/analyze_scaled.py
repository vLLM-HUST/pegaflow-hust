"""Aggregate paired cohorts while retaining scheduler and transport qualifications."""

import argparse
import json
import random
from collections import defaultdict
from itertools import pairwise
from pathlib import Path

from common import distribution, read_json, read_jsonl, write_json

ARMS = ("rdma-burst", "rdma-smooth", "local_copy-burst", "local_copy-smooth")


def stats(values):
    value = distribution(values)
    value["mean"] = sum(values) / len(values) if values else None
    return value


def analyze(root):
    contract_path = root / "campaign-contract.json"
    contract = read_json(contract_path) if contract_path.exists() else {}
    arms = defaultdict(lambda: defaultdict(list))
    pairs = []
    for marker in sorted(root.glob("cohort-*/complete.json")):
        summaries = read_json(marker)["summaries"]
        pair = {"cohort": marker.parent.name}
        for arm in ARMS:
            path = Path(summaries[arm]["result_dir"])
            clients = read_jsonl(path / "client.jsonl")
            events = read_jsonl(path / "observed.jsonl")
            eligible = {
                e["request_id"]: e["monotonic_ns"]
                for e in events
                if e["event"] == "bundle_eligible"
            }
            posted = {
                e["request_id"]: e["monotonic_ns"]
                for e in events
                if e["event"] == "bundle_posted"
            }
            completed = {
                e["request_id"]: e["monotonic_ns"]
                for e in events
                if e["event"] == "bundle_completed"
            }
            tg = max(eligible.values())
            for row in clients:
                rid = row["request_id"]
                arms[arm]["ttft_seconds"].append(row["ttft_seconds"])
                arms[arm]["cohort_wait_seconds"].append((tg - eligible[rid]) / 1e9)
                arms[arm]["ttft_without_cohort_wait_seconds"].append(
                    row["ttft_seconds"] - (tg - eligible[rid]) / 1e9
                )
                arms[arm]["transfer_completion_ms"].append(
                    (completed[rid] - posted[rid]) / 1e6
                )
                arms[arm]["cache_access_ms"].append(
                    (completed[rid] - eligible[rid]) / 1e6
                )
            injected = min(r["started_monotonic_ns"] for r in clients) / 1e9
            for observer in read_jsonl(path / "observers/client.jsonl"):
                tokens = observer["token_events"]
                origin = observer["started_monotonic_ns"] / 1e9
                for a, b in pairwise(tokens):
                    if origin + b["offset_seconds"] >= injected:
                        arms[arm]["observer_itl_ms"].append(
                            (b["offset_seconds"] - a["offset_seconds"]) * 1000
                        )
            arms[arm]["posting_spread_ms"].extend(
                e["posting_spread_ns"] / 1e6
                for e in events
                if e["event"] == "cohort_posting_spread"
            )
            pair[arm] = summaries[arm]["ttft_mean_seconds"]
        pairs.append(pair)
    effects = {}
    rng = random.Random(17)
    for backend in ("rdma", "local_copy"):
        differences = [p[f"{backend}-smooth"] - p[f"{backend}-burst"] for p in pairs]
        blocks = [differences[i : i + 16] for i in range(0, len(differences) - 15, 16)]
        bootstrap = []
        if len(blocks) >= 8:
            means = [sum(block) / len(block) for block in blocks]
            bootstrap = [
                sum(rng.choice(means) for _ in means) / len(means) for _ in range(5000)
            ]
        bootstrap.sort()
        effects[backend] = {
            "paired_cohort_ttft_difference_seconds": stats(differences),
            "blocks": len(blocks),
            "block_bootstrap_mean_ci95": [bootstrap[125], bootstrap[4874]]
            if bootstrap
            else None,
            "bootstrap_scope": "16 paired cohorts per time block; model processes reused",
        }
    return {
        "complete_cohorts": len(pairs),
        "foreground_requests": sum(len(data["ttft_seconds"]) for data in arms.values()),
        "arms": {
            arm: {name: stats(values) for name, values in data.items()}
            for arm, data in arms.items()
        },
        "paired_effects": effects,
        "output_scope": "one-token foreground TTFT and existing decoder ITL",
        "transport_scope": (
            "software injection budget; completion includes transport submission queueing"
            if contract.get("rdma_rate_bps", 1)
            else "native RDMA; completion includes transport submission queueing"
        ),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = analyze(args.root)
    if args.output:
        write_json(args.output, result)
    print(json.dumps(result, indent=2))
