"""Recompute the contention pilot from its portable per-arm audit records."""

import argparse
import bisect
import gzip
import hashlib
import json
import math
import statistics
from collections import Counter, defaultdict
from itertools import pairwise
from pathlib import Path

from common import distribution, read_json, write_json
from freeze_plan import validate_plan

ARMS = ("rdma-burst", "rdma-smooth", "local_copy-burst", "local_copy-smooth")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def compressed(path):
    return json.loads(gzip.decompress(path.read_bytes()))


def compare(actual, expected, label):
    if isinstance(expected, dict):
        for key, value in expected.items():
            compare(actual[key], value, f"{label}.{key}")
    elif expected is None:
        require(actual is None, label)
    else:
        require(math.isclose(actual, expected, rel_tol=1e-9, abs_tol=1e-6), label)


def stats(values):
    return {**distribution(values), "mean": statistics.mean(values) if values else None}


def verify(root):
    manifest = read_json(root / "manifest.json")
    for name, entry in manifest.items():
        path = (root / name).resolve()
        require(
            path.is_relative_to(root.resolve()), "Manifest path escapes evidence root"
        )
        data = path.read_bytes()
        require(len(data) == entry["bytes"], f"Size mismatch: {name}")
        require(
            hashlib.sha256(data).hexdigest() == entry["sha256"],
            f"Hash mismatch: {name}",
        )
    expected = read_json(root / "pilot-analysis.json")
    context = read_json(root / "pilot-context-analysis.json")
    telemetry = compressed(root / "telemetry.json.gz")
    first_work = {
        (r["request_id"], r["tp_rank"]): r for r in telemetry["first_foreground_work"]
    }
    resources = telemetry["resource_samples"]
    times = [r["unix_ns"] for r in resources]
    steps = telemetry["decoder_step_unix_ns"]
    collected = defaultdict(lambda: defaultdict(list))
    phases = defaultdict(lambda: defaultdict(list))
    cpu = defaultdict(lambda: defaultdict(list))
    transport = defaultdict(lambda: defaultdict(float))
    network = defaultdict(lambda: defaultdict(int))
    checkpoint = Counter()
    datasets = Counter()
    pairs, all_cases = [], set()

    def brackets(start, end):
        lo = bisect.bisect_right(times, start) - 1
        hi = bisect.bisect_left(times, end)
        require(lo >= 0 and hi < len(times), "Telemetry does not bracket the window")
        return resources[lo], resources[hi]

    for directory in sorted(root.glob("cohort-*")):
        cohort = compressed(directory / "cohort.json.gz")
        plan = cohort["plan"]
        validate_plan(plan)
        planned = {r["request_id"]: r for r in plan["requests"]}
        require(len(planned) == 16, "Incomplete sixteen-request cohort")
        require(not all_cases.intersection(planned), "Cases repeated between cohorts")
        all_cases.update(planned)
        datasets.update(c["dataset"] for c in cohort["cases"])
        pair = {}
        for arm in ARMS:
            data = compressed(directory / (arm + ".json.gz"))
            clients = data["client.jsonl"]
            require(len(clients) == len(planned), "Foreground count mismatch")
            require(
                {r["request_id"] for r in clients} == set(planned),
                "Foreground identity mismatch",
            )
            events = data["observed.jsonl"]
            maps = {}
            for name in (
                "bundle_observed",
                "bundle_eligible",
                "bundle_posted",
                "bundle_completed",
            ):
                rows = [e for e in events if e["event"] == name]
                maps[name] = {r["request_id"]: r for r in rows}
                require(
                    len(rows) == len(planned) and set(maps[name]) == set(planned),
                    f"Missing or repeated {name}",
                )
            eligible, posted, completed = (
                maps[n]
                for n in ("bundle_eligible", "bundle_posted", "bundle_completed")
            )
            tg = max(e["monotonic_ns"] for e in eligible.values())
            window = data["measurement-window.json"]
            require(
                window["cohort_ready_unix_ns"]
                == max(e["wall_time_unix_ns"] for e in eligible.values()),
                "Ready time mismatch",
            )
            for client in clients:
                rid = client["request_id"]
                require(
                    client["status"] == "ok"
                    and client["usage"]["prompt_tokens"] == 4097
                    and client["usage"]["completion_tokens"] == 1,
                    "Foreground failed or changed workload",
                )
                require(
                    maps["bundle_observed"][rid]["operations"]
                    == planned[rid]["operations"],
                    "Measured operation manifest changed",
                )
                value = completed[rid]
                require(
                    value["status"] == "ok"
                    and value["completed_payload_bytes"]
                    == sum(
                        op["logical_payload_bytes"] for op in planned[rid]["operations"]
                    )
                    == 148275200,
                    "Incomplete cache payload",
                )
                require(
                    eligible[rid]["monotonic_ns"]
                    <= posted[rid]["monotonic_ns"]
                    <= value["monotonic_ns"],
                    "Invalid native event order",
                )
                wait = (tg - eligible[rid]["monotonic_ns"]) / 1e9
                for name, number in (
                    ("ttft_seconds", client["ttft_seconds"]),
                    ("cohort_wait_seconds", wait),
                    ("ttft_without_cohort_wait_seconds", client["ttft_seconds"] - wait),
                    (
                        "transfer_completion_ms",
                        (value["monotonic_ns"] - posted[rid]["monotonic_ns"]) / 1e6,
                    ),
                    (
                        "cache_access_ms",
                        (value["monotonic_ns"] - eligible[rid]["monotonic_ns"]) / 1e6,
                    ),
                ):
                    collected[arm][name].append(number)
                work = [first_work[(value["raw_request_id"], rank)] for rank in (0, 1)]
                for row in work:
                    require(
                        row["computed_tokens"] == 4096 and row["scheduled_tokens"] == 1,
                        "Checkpoint was not reused",
                    )
                    checkpoint[arm] += 1
                phases[arm]["cache_complete_to_model_work_ms"].append(
                    (max(w["unix_ns"] for w in work) - value["wall_time_unix_ns"]) / 1e6
                )
            collected[arm]["posting_spread_ms"].extend(
                e["posting_spread_ns"] / 1e6
                for e in events
                if e["event"] == "cohort_posting_spread"
            )
            start, end = (
                window["cohort_ready_unix_ns"],
                window["event_window_ends_unix_ns"],
            )
            phases[arm]["decode_step_interval_in_event_window_ms"].extend(
                (b - a) / 1e6 for a, b in pairwise(steps) if a <= end and b >= start
            )
            injection = min(r["started_monotonic_ns"] for r in clients) / 1e9
            for observer in data["observers/client.jsonl"]:
                for a, b in pairwise(observer["token_events"]):
                    gap = (b["offset_seconds"] - a["offset_seconds"]) * 1000
                    if (
                        observer["started_monotonic_ns"] / 1e9 + b["offset_seconds"]
                        >= injection
                    ):
                        collected[arm]["observer_itl_ms"].append(gap)
                    lo = observer["started_unix_ns"] + a["offset_seconds"] * 1e9
                    hi = observer["started_unix_ns"] + b["offset_seconds"] * 1e9
                    if (
                        window["baseline_started_unix_ns"]
                        <= lo
                        <= hi
                        <= window["foreground_started_unix_ns"]
                    ):
                        phases[arm]["sse_baseline_ms"].append((hi - lo) / 1e6)
                    if lo <= end and hi >= start:
                        phases[arm]["sse_event_window_ms"].append((hi - lo) / 1e6)
            phases[arm]["competitor_reported_gbps"].extend(
                r["bandwidth_gbps"] for r in data["network-background.json"]["samples"]
            )
            before, after = brackets(
                window["foreground_started_unix_ns"],
                window["observation_finished_unix_ns"] - 1_000_000_000,
            )
            for kind in ("queue", "service"):
                count = (
                    after["engine_read"][kind + "_count"]
                    - before["engine_read"][kind + "_count"]
                )
                total = (
                    after["engine_read"][kind + "_sum"]
                    - before["engine_read"][kind + "_sum"]
                )
                transport[arm][kind + "_batches"] += count
                transport[arm][kind + "_seconds"] += total
                if count:
                    phases[arm][kind + "_seconds_per_batch_ms"].append(
                        total * 1000 / count
                    )
            before, after = brackets(start, end)
            elapsed = (after["unix_ns"] - before["unix_ns"]) / 1e9
            received = int(after["rdma"]["port_rcv_data"]) - int(
                before["rdma"]["port_rcv_data"]
            )
            phases[arm]["physical_port_receive_gbps"].append(
                received * 4 * 8 / elapsed / 1e9
            )
            for name in expected["network_counter_deltas_in_sample_brackets"][arm]:
                network[arm][name] += int(after["rdma"][name]) - int(
                    before["rdma"][name]
                )
            cpu[arm]["used_cpu_cores"].append(
                (after["cpu"]["usage_usec"] - before["cpu"]["usage_usec"])
                / 1e6
                / elapsed
            )
            cpu[arm]["throttled_period_fraction"].append(
                (after["cpu"]["nr_throttled"] - before["cpu"]["nr_throttled"])
                / max(1, after["cpu"]["nr_periods"] - before["cpu"]["nr_periods"])
            )
            pair[arm] = statistics.mean(r["ttft_seconds"] for r in clients)
        pairs.append(pair)

    require(
        len(pairs) == expected["complete_cohorts"] == 8,
        "Complete cohort count mismatch",
    )
    require(
        sum(len(d["ttft_seconds"]) for d in collected.values())
        == expected["foreground_requests"]
        == 512,
        "Request count mismatch",
    )
    require(
        dict(datasets) == context["executed_dataset_counts"],
        "Dataset composition mismatch",
    )
    for arm in ARMS:
        compare(
            {k: stats(v) for k, v in collected[arm].items()}, expected["arms"][arm], arm
        )
        compare(
            {k: stats(v) for k, v in phases[arm].items()}, expected["phases"][arm], arm
        )
        compare(dict(transport[arm]), expected["transport_batch_metrics"][arm], arm)
        compare(
            dict(network[arm]),
            expected["network_counter_deltas_in_sample_brackets"][arm],
            arm,
        )
        require(
            checkpoint[arm]
            == expected["checkpoint_validation"]["first_work_valid_per_arm"][arm],
            "Checkpoint count mismatch",
        )
        for key, values in cpu[arm].items():
            compare(
                statistics.mean(values),
                context["cpu_event_window"][arm][key]["mean"],
                arm,
            )
    for backend in ("rdma", "local_copy"):
        values = [p[backend + "-smooth"] - p[backend + "-burst"] for p in pairs]
        compare(
            {**stats(values), "negative_cohorts": sum(v < 0 for v in values)},
            expected["paired_cohort_effects"][backend],
            backend,
        )
    for index, pair in enumerate(pairs):
        compare(pair, expected["cohort_means"][index], "cohort means")
        effect = (pair["rdma-burst"] - pair["rdma-smooth"]) - (
            pair["local_copy-burst"] - pair["local_copy-smooth"]
        )
        compare(
            effect,
            expected["network_interaction_seconds_by_cohort"][index],
            "interaction",
        )
    return {
        "status": "PASS",
        "files_verified": len(manifest),
        "complete_cohorts": len(pairs),
        "foreground_requests": 512,
        "first_model_work_records": sum(checkpoint.values()),
        "checked": [
            "manifest",
            "frozen operations",
            "workload",
            "payload",
            "checkpoint reuse",
            "TTFT and cache distributions",
            "decoder windows",
            "QP metrics",
            "CPU quota",
            "port counters",
        ],
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = verify(args.root)
    if args.output:
        write_json(args.output, result)
    print(json.dumps(result, indent=2))
