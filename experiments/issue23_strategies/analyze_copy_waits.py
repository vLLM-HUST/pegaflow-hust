"""Observed P-copy gate exposure, not RDMA congestion or counterfactual savings.

The exact P-stage dequeue event is not archived. A conservative FIFO-ready lower
bound uses completed routing and the preceding P API completion. Unlogged host
bookkeeping can occupy the resulting window. Classify overlapping gate/link
intervals jointly, never sum API residence and device iterations as two holds.
"""

import argparse
import hashlib
import itertools
import json
from collections import Counter, defaultdict
from pathlib import Path


def partition(start, end, intervals):
    """Disjoint resource-exposure cells, excluding intervals outside the window."""
    clipped = [
        (max(start, a), min(end, b), kind)
        for a, b, kind in intervals
        if a < end and b > start
    ]
    points = sorted({start, end, *(v for a, b, _ in clipped for v in (a, b))})
    result = Counter()
    for a, b in itertools.pairwise(points):
        active = sorted({kind for x, y, kind in clipped if x < b and y > a})
        result["+".join(active) if active else "unexplained_or_bookkeeping"] += b - a
    assert abs(sum(result.values()) - (end - start)) < 1e-7
    return dict(result)


def resource_holds(arm):
    holds = defaultdict(list)
    starts = {}
    for r in arm["resource_operation_ledger"]:
        name = r["operation"]
        if name not in ("P_api", "history", "handoff"):
            continue  # D_api is a batched residence, NOT an exclusive hold.
        key = r["key"], name
        if r["event"] == "start":
            assert key not in starts
            starts[key] = r
        elif r["event"] == "finish":
            before = starts.pop(key)
            assert before["resources"] == r["resources"]
            for resource in r["resources"]:
                holds[resource].append(
                    (before["seconds"], r["seconds"], r["key"], name)
                )
    assert not starts
    for r in arm["batch_iterations"]:
        holds[f"gate{r['node']}"].append(
            (r["start"], r["end"], tuple(p[0] for p in r["pieces"]), r["kind"])
        )
    return holds


def analyze_arm(arm):
    holds = resource_holds(arm)
    decisions = {r["id"]: r for r in arm["decisions"]}
    completed_route = {
        d["id"]: r["completed"]
        for d, r in zip(
            arm["decisions"], arm["control_work_observations"]["route"], strict=True
        )
    }
    P = {r["id"]: r for r in arm["stages"] if r["node"] < 2}
    starts = {
        (r["key"], r["operation"]): r
        for r in arm["resource_operation_ledger"]
        if r["event"] == "start"
    }
    rows = []
    for copy in arm["resource_events"]:
        if copy["operation"] != "history":
            continue
        key, node = copy["request"], copy["node"]
        record = starts[key, "history"]
        source = next(
            int(r[4:])
            for r in record["resources"]
            if r.startswith("gate") and r != f"gate{node}"
        )
        link = next(r for r in record["resources"] if r.startswith("link:"))
        earliest = max(
            completed_route[key],
            max(
                (
                    r["completed"]
                    for k, r in P.items()
                    if k != key
                    and r["node"] == node
                    and r["completed"] <= copy["start"]
                ),
                default=0,
            ),
        )
        assert earliest <= copy["start"] + 1e-7
        earliest = min(earliest, copy["start"])
        blockers = []
        for kind, resource in (
            ("source_gate", f"gate{source}"),
            ("destination_gate", f"gate{node}"),
            ("direction_slot", link),
        ):
            for a, b, owner, operation in holds[resource]:
                if owner == key or a >= copy["start"] or b <= earliest:
                    continue
                blockers.append(
                    {
                        "kind": kind,
                        "owner": owner,
                        "operation": operation,
                        "start": a,
                        "end": b,
                        "overlap": min(b, copy["start"]) - max(a, earliest),
                        "owner_route_after_target_route": isinstance(owner, str)
                        and owner in decisions
                        and decisions[owner]["origin"] > decisions[key]["origin"],
                    }
                )
        exposure = partition(
            earliest,
            copy["start"],
            [(r["start"], r["end"], r["kind"]) for r in blockers],
        )
        rows.append(
            {
                "id": key,
                "source": source,
                "destination": node,
                "route_origin": decisions[key]["origin"],
                "route_completed": completed_route[key],
                "FIFO_ready_lower_bound_not_exact_dequeue": earliest,
                "copy_start": copy["start"],
                "copy_end": copy["end"],
                "copy_seconds": copy["end"] - copy["start"],
                "post_FIFO_lower_bound_wait": copy["start"] - earliest,
                "partition": exposure,
                "blockers": blockers,
            }
        )
    totals = Counter()
    for r in rows:
        totals.update(r["partition"])
    return {
        "copies": len(rows),
        "copy_execution_seconds_sum": sum(r["copy_seconds"] for r in rows),
        "post_FIFO_lower_bound_wait_sum_NOT_wall_gain": sum(
            r["post_FIFO_lower_bound_wait"] for r in rows
        ),
        "partition_sums_NOT_wall_gain": dict(totals),
        "younger_source_blocked_copies": sum(
            any(
                b["kind"] == "source_gate" and b["owner_route_after_target_route"]
                for b in r["blockers"]
            )
            for r in rows
        ),
        "largest_waits": sorted(
            rows, key=lambda r: r["post_FIFO_lower_bound_wait"], reverse=True
        )[:5],
        "requests": rows,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    cases = []
    for index in (1, 2, 3):
        p = args.result / f"case-{index:02d}.json"
        raw = json.loads(p.read_bytes())
        cases.append(
            {
                "rate": raw["rate"],
                "raw_sha256": hashlib.sha256(p.read_bytes()).hexdigest(),
                "arms": {
                    name: analyze_arm(raw["arms"][name])
                    for name in ("reference", "parent", "ours")
                },
            }
        )
    report = {
        "CPU_only": True,
        "trace_conditioned_diagnostic_NOT_new_policy_performance": True,
        "FIFO_dequeue_not_logged": True,
        "gate_exposure_not_RDMA_congestion": True,
        "cases": cases,
    }
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2)
    print(
        json.dumps(
            [
                {
                    "rate": c["rate"],
                    "arms": {
                        name: {
                            k: v
                            for k, v in a.items()
                            if k not in ("requests", "largest_waits")
                        }
                        for name, a in c["arms"].items()
                    },
                }
                for c in cases
            ]
        )
    )


if __name__ == "__main__":
    main()
