"""Past-only P recovery quotes at a conservative FIFO-eligibility checkpoint.

This is a fixed-trace mechanism screen, NOT policy feedback or net TTFT benefit.
The actual dequeue/cache snapshot was not logged. The no-copy alternative is
always zero-prefix recomputation; remote-prefix feasibility is conditional.
Only completed P phases/copies train the fit. Live age is not a completion.
Neither the target's later service time nor the active source's future end is
used to quote a choice. Destination/link already busy cases remain unresolved.
"""

import argparse
import hashlib
import json
from pathlib import Path

import simulate_continuous_batch as sim
from analyze_copy_waits import analyze_arm
from pegaflow.operation_survival import conditional_remaining
from pegaflow.resource_queue import direction


def quote_recovery(costs, node, tokens, prefix, source, remaining):
    """Compare physical P completion cost; subsequent global tails are separate."""
    payload_upper_bound = prefix * costs.bytes_per_token
    copy = costs.copy_seconds(source, node, payload_upper_bound)
    warm = costs.compute(node, tokens, prefix)
    cold = costs.compute(node, tokens, 0)
    return {
        "prefix_command": prefix,
        "payload_upper_bound": payload_upper_bound,
        "source_remaining": remaining,
        "copy_quote": copy,
        "warm_P_including_H2D_sync": warm,
        "cold_P_including_sync": cold,
        "remote_P_completion": remaining + copy + warm,
        "cold_P_completion": cold,
        "cold_margin_before_new_control_NOT_net_TTFT": remaining + copy + warm - cold,
    }


def active_operations(ledger, now):
    live = {}
    for r in ledger:
        if r["seconds"] > now:
            continue
        key = r["key"], r["operation"]
        if r["event"] == "start":
            live[key] = r
        elif r["event"] == "finish":
            live.pop(key, None)
    return list(live.values())


def completed_samples(ledger, node, now):
    starts, samples = {}, []
    for r in ledger:
        if r["seconds"] > now or r["operation"] != "P_api":
            continue
        if f"gate{node}" not in r["resources"]:
            continue
        if r["event"] == "start":
            starts[r["key"]] = r
        else:
            before = starts.pop(r["key"])
            samples.append(
                (before["predicted_seconds"], r["seconds"] - before["seconds"])
            )
    return samples


def analyze(arm, initial, seed):
    waits = analyze_arm(arm)
    decisions = {r["id"]: r for r in arm["decisions"]}
    stages = {r["id"]: r for r in arm["stages"] if r["node"] < 2}
    ledger = arm["resource_operation_ledger"]
    model = sim.parent.core.seed_operations(initial, seed, "phase")
    frozen_seed = model.freeze(0)
    seed_samples = {0: [], 1: []}
    for node, result, wire in model.phase_calibration:
        if node < 2:
            seed_samples[node].append(
                (
                    frozen_seed.compute(
                        node, result["prompt_tokens"], result["num_cached_tokens"]
                    ),
                    wire["complete_seconds"],
                )
            )
    history = sorted(
        [(s["completed"], "P", s) for s in stages.values()]
        + [
            (a["at"], "copy", a) for a in arm["actions"] if a["kind"] == "selected_copy"
        ],
        key=lambda r: r[0],
    )
    cursor, rows = 0, []
    for wait in sorted(
        waits["requests"], key=lambda r: r["FIFO_ready_lower_bound_not_exact_dequeue"]
    ):
        key = wait["id"]
        origin = wait["FIFO_ready_lower_bound_not_exact_dequeue"]
        stage, initial_route = stages[key], decisions[key]["initial_forecast"]
        while cursor < len(history) and history[cursor][0] <= origin:
            completed, kind, r = history[cursor]
            if kind == "P":
                result = {
                    "id": r["id"],
                    "prompt_tokens": r["tokens"],
                    "num_cached_tokens": r["prefix"],
                    "raw_metrics": {
                        "queued_ts": 0.0,
                        "scheduled_ts": r["restore_path_not_pure_H2D"],
                        "first_token_ts": r["first"],
                        "last_token_ts": r["full"],
                    },
                }
                model.observe(
                    r["node"],
                    result,
                    {"first_seconds": r["first"], "complete_seconds": r["full"]},
                    completed,
                )
            else:
                model.observe_copy(
                    r["source"],
                    r["destination"],
                    r["payload"],
                    r["seconds"],
                    0,
                    completed,
                )
            cursor += 1
        costs = model.freeze(origin)
        source, node = wait["source"], wait["destination"]
        row = {
            "id": key,
            "origin_lower_bound": origin,
            "source": source,
            "destination": node,
            "model_latest": costs.latest_completed_seconds,
            "model_version": costs.version,
            "observed_copy_wait": wait["post_FIFO_lower_bound_wait"],
            "observation_NOT_policy_input": {
                "copy_seconds": wait["copy_seconds"],
                "blockers": wait["blockers"],
            },
        }
        assert costs.latest_completed_seconds <= origin
        active = active_operations(ledger, origin)
        # Only P exclusive API is modeled here. D residence must not be used as
        # a device hold, and an active copy's future duration is not observable.
        blockers = [r for r in active if f"gate{source}" in r["resources"]]
        if source >= 2 or len(blockers) != 1 or blockers[0]["operation"] != "P_api":
            row["status"] = "outside_active_P_source_scope"
        elif any(
            f"gate{node}" in r["resources"]
            or "link:" + direction(source, node) in r["resources"]
            for r in active
        ):
            row["status"] = "destination_or_link_busy_unresolved"
        elif (
            initial_route["source"],
            initial_route["p"],
            initial_route["P_prefix_command"],
        ) != (source, node, stage["prefix"]):
            row["status"] = "original_quote_and_executed_command_differ_no_substitution"
        else:
            live = blockers[0]
            samples = seed_samples[source] + completed_samples(ledger, source, origin)
            age = origin - live["seconds"]
            residual = conditional_remaining(samples, live["predicted_seconds"], age)
            row.update(
                status="conditional_quote_not_global_decision",
                live_source_request=live["key"],
                live_age=age,
                issued_source_seconds=live["predicted_seconds"],
                completed_residual_samples=len(samples),
                **quote_recovery(
                    costs,
                    node,
                    stage["tokens"],
                    initial_route["P_prefix_command"],
                    source,
                    residual,
                ),
            )
        rows.append(row)
    quoted = [r for r in rows if r["status"] == "conditional_quote_not_global_decision"]
    return {
        "requests": rows,
        "quoted": len(quoted),
        "positive_cold_margins": sum(
            r["cold_margin_before_new_control_NOT_net_TTFT"] > 0 for r in quoted
        ),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("result", "inputs", "seed", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    data = json.loads(args.inputs.read_bytes())
    values = dict(data["costs"])
    values["service"] = tuple(
        sim.parent.core.ObservedLatencyModel(**v) for v in values["service"]
    )
    values["decode_seconds"] = tuple(values["decode_seconds"])
    initial = sim.Costs(**values)
    report = {
        "CPU_only": True,
        "fixed_trace_NOT_feedback_performance": True,
        "P_FIFO_checkpoint_is_lower_bound": True,
        "remote_feasibility_conditional": True,
        "new_control_cost_and_global_tail_not_evaluated": True,
        "cases": [],
    }
    for index in (1, 2, 3):
        p = args.result / f"case-{index:02d}.json"
        raw = json.loads(p.read_bytes())
        row = {
            "rate": raw["rate"],
            "raw_sha256": hashlib.sha256(p.read_bytes()).hexdigest(),
            "arms": {},
        }
        for name in ("reference", "parent", "ours"):
            row["arms"][name] = analyze(raw["arms"][name], initial, args.seed)
        report["cases"].append(row)
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2)
    print(
        json.dumps(
            [
                {
                    "rate": c["rate"],
                    "arms": {
                        n: {k: v for k, v in a.items() if k != "requests"}
                        for n, a in c["arms"].items()
                    },
                }
                for c in report["cases"]
            ]
        )
    )


if __name__ == "__main__":
    main()
