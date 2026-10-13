"""Join client and process telemetry to distinguish reuse, queueing and transfer waits."""

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path

from common import distribution, read_jsonl, write_json


def find_client_id(raw, known, observed_ns):
    # Separate repeated request IDs across runs using the shared host clock.
    for rid, client in known.items():
        if raw != rid and not raw.startswith("cmpl-" + rid + "-0-"):
            continue
        started = client["started_monotonic_ns"]
        finished = started + int(client["e2e_seconds"] * 1e9) + 1_000_000
        if started <= observed_ns <= finished:
            return rid
    return None


def metric_total(folder, label, endpoint, metric):
    return sum(
        float(line.rsplit(" ", 1)[-1])
        for path in folder.glob(f"{label}-{endpoint}.prom")
        for line in path.read_text().splitlines()
        if line.startswith(metric + "{") or line.startswith(metric + " ")
    )


def counter_delta(folder, endpoint, metric):
    return metric_total(folder, "after", endpoint, metric) - metric_total(
        folder, "before", endpoint, metric
    )


def analyze(root, telemetry_dir):
    clients, locations = {}, {}
    for path in sorted(root.glob("round-*-*/client.jsonl")):
        if not any(
            path.parent.name.endswith("-" + a) for a in ("cold", "local_host", "remote_host")
        ):
            continue
        for row in read_jsonl(path):
            clients[row["request_id"]] = row
            locations[row["request_id"]] = path.parent
    work, queries, loads, drops = (
        defaultdict(list),
        defaultdict(list),
        defaultdict(list),
        [],
    )
    for path in sorted(telemetry_dir.glob("*.jsonl")):
        for event in read_jsonl(path):
            if event["event"] == "model_work" and event.get("tp_rank") == 0:
                for request in event["requests"]:
                    rid = find_client_id(request["request_id"], clients, event["monotonic_ns"])
                    if rid and request["scheduled_tokens"]:
                        work[rid].append({**request, "monotonic_ns": event["monotonic_ns"]})
            elif event["event"] == "cache_query_ready":
                rid = find_client_id(event["request_id"], clients, event["monotonic_ns"])
                if rid:
                    queries[rid].append(event)
            elif event["event"] == "load_completed" and event.get("tp_rank") == 0:
                for raw in event["request_ids"]:
                    rid = find_client_id(raw, clients, event["monotonic_ns"])
                    if rid:
                        loads[rid].append(event)
            elif event["event"] == "telemetry_closed":
                drops.append({"path": path.name, "dropped_events": event["dropped_events"]})
    details, grouped, semantic_errors = [], defaultdict(list), []
    output_identity = defaultdict(dict)
    for rid, client in clients.items():
        arm = next(a for a in ("cold", "local_host", "remote_host") if rid.endswith("-" + a))
        actual = sorted(work[rid], key=lambda row: row["monotonic_ns"])
        first = actual[0] if actual else None
        prefill = sum(
            min(w["scheduled_tokens"], max(0, w["prompt_tokens"] - w["computed_tokens"]))
            for w in actual
        )
        observed_loads = loads[rid]
        detail = {
            "request_id": rid,
            "arm": arm,
            "ttft_seconds": client.get("ttft_seconds"),
            "e2e_seconds": client["e2e_seconds"],
            "output_tokens": client.get("usage", {}).get("completion_tokens"),
            "first_forward_computed_tokens": first["computed_tokens"] if first else None,
            "actual_prefill_tokens": prefill if first else None,
            "first_forward_wait_seconds": (first["monotonic_ns"] - client["started_monotonic_ns"])
            / 1e9
            if first
            else None,
            "cache_query_wait_ms": max((q["wait_ms"] for q in queries[rid]), default=None),
            "load_observation_ms": max(
                (load["duration_ms"] for load in observed_loads), default=None
            ),
            "load_succeeded": all(load["success"] for load in observed_loads)
            if observed_loads
            else None,
            "output_sha256": client.get("output_sha256"),
            "dataset": "frontier-science" if "frontier-smoke" in rid else "mmlu-pro",
            "round": int(re.search(r"-r(\d+)-", rid).group(1)),
        }
        if observed_loads and not detail["load_succeeded"]:
            semantic_errors.append({"request_id": rid, "reason": "native state load failed"})
        if first and not 0 <= first["computed_tokens"] <= first["prompt_tokens"]:
            semantic_errors.append(
                {"request_id": rid, "reason": "restored boundary exceeds prompt"}
            )
        detail["real_reuse"] = bool(first and first["computed_tokens"] > 0)
        details.append(detail)
        grouped[arm].append(detail)
        output_identity[re.sub(r"-(cold|local_host|remote_host)$", "", rid)][arm] = client.get(
            "output_sha256"
        )
    aggregates = {}
    for arm, rows in grouped.items():
        aggregates[arm] = {
            "requests": len(rows),
            "real_reuse_requests": sum(r["real_reuse"] for r in rows),
            **{
                key: distribution([r[key] for r in rows if r[key] is not None])
                for key in (
                    "ttft_seconds",
                    "e2e_seconds",
                    "actual_prefill_tokens",
                    "first_forward_wait_seconds",
                    "cache_query_wait_ms",
                    "load_observation_ms",
                )
            },
        }
    completed = [x for x in output_identity.values() if len(x) == 3]
    transfer_metrics = []
    for folder in sorted(set(locations.values())):
        delta = {
            metric: counter_delta(folder, "decode-rank*", metric)
            for metric in ("pegaflow_rdma_fetch_bytes_total", "pegaflow_load_bytes_total")
        }
        durations = {}
        for metric in ("pegaflow_rdma_fetch_duration_seconds", "pegaflow_load_duration_seconds"):
            count = counter_delta(folder, "decode-rank*", metric + "_count")
            total = counter_delta(folder, "decode-rank*", metric + "_sum")
            durations[metric] = {
                "count": count,
                "sum_seconds": total,
                "mean_ms": 1000 * total / count if count else None,
            }
        transfer_metrics.append(
            {
                "run": folder.name,
                "counter_delta_bytes": delta,
                "native_duration_metrics": durations,
                "decode_preemptions": counter_delta(folder, "decode", "vllm:num_preemptions_total"),
                "source_evictions_at_arm_start": metric_total(
                    folder, "before", "prefill-rank*", "pegaflow_cache_block_evictions_total"
                ),
            }
        )
    cohort_aggregates = {
        arm: {
            dataset: {
                "requests": len(selected),
                "real_reuse_requests": sum(r["real_reuse"] for r in selected),
                "ttft_seconds": distribution([r["ttft_seconds"] for r in selected]),
                "first_forward_wait_seconds": distribution(
                    [
                        r["first_forward_wait_seconds"]
                        for r in selected
                        if r["first_forward_wait_seconds"] is not None
                    ]
                ),
            }
            for dataset in ("mmlu-pro", "frontier-science")
            if (selected := [r for r in rows if r["dataset"] == dataset])
        }
        for arm, rows in grouped.items()
    }
    return {
        "aggregates": aggregates,
        "requests": details,
        "semantic_errors": semantic_errors,
        "output_comparison": {
            "matched_request_triplets": len(completed),
            "identical_text_triplets": sum(len(set(x.values())) == 1 for x in completed),
            "policy": "output differences are observations, not timing rejection criteria",
        },
        "telemetry_closed_records": drops,
        "transfer_metrics": transfer_metrics,
        "cohort_aggregates": cohort_aggregates,
        "clock_note": "Client and worker observations were collected on the decode host; CPU span and load completion are observation times.",
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--telemetry", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    result = analyze(args.root, args.telemetry)
    write_json(args.output, result)
    print(
        json.dumps(
            {
                "aggregates": result["aggregates"],
                "output_comparison": result["output_comparison"],
                "semantic_errors": result["semantic_errors"],
            },
            indent=2,
        )
    )
