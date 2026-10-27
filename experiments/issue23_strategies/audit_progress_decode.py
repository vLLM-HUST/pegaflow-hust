"""Verify charged serialized advice, cancellation, FIFO and owned-D commits."""

import argparse
import itertools
import json
from pathlib import Path

from audit_late_decode import close


def audit(arm):
    records = arm["decode_advice"]
    decisions = {r["id"]: r for r in arm["decisions"]}
    outputs = {r["id"]: r for r in arm["per_request"]}
    controls = {
        r["request"]: r for r in arm["resource_events"] if r["operation"] == "D_control"
    }
    stages = {(r["id"], r["node"] < 2): r for r in arm["stages"]}
    assert len({r["id"] for r in records}) == len(records)
    intervals = sorted((r["origin"], r["resolved"]) for r in records)
    assert all(a[1] <= b[0] + 1e-8 for a, b in itertools.pairwise(intervals))
    for r in records:
        key, origin = r["id"], r["origin"]
        known = [v for v in records if v["decision_ready"] <= origin and v is not r]
        close(
            r["predicted_CPU"],
            sum(v["CPU_wall"] for v in known) / len(known) if known else 0,
        )
        assert len(known) == r["completed_CPU_samples"]
        assert r["CPU_latest"] == max(
            (v["decision_ready"] for v in known), default=None
        )
        assert r["model_latest"] <= origin
        close(stages[key, True]["completed"], r["P_completed"])
        assert r["P_completed"] <= origin
        close(r["decision_ready"], origin + r["CPU_wall"] + r["RPC_seconds"])
        assert r["resolved"] >= r["decision_ready"] + r["commit_CPU_wall"] - 1e-8
        assert arm["makespan"] >= r["resolved"]
        for d in decisions.values():
            # Ordinary route search cannot start inside optional advice's lane.
            assert not origin <= d["origin"] < r["resolved"] - 1e-8
        initial = decisions[key]["initial_forecast"]
        assert initial["d"] == r["before"]
        assert outputs[key]["p"] == initial["p"] == stages[key, True]["node"]
        prior, after = r["commit_queue_before"], r["commit_queue_after"]
        keep, *options = r["evaluations"]
        assert (
            keep["kind"] == "keep" and keep["delay"] == 0 and keep["d"] == r["before"]
        )
        assert len(r["evaluations"]) <= 64
        if r["selected"] != r["before"]:
            assert any(
                v["objective"] < keep["objective"] and v["d"] == r["selected"]
                for v in options
            )
        if r["status"] == "move":
            assert outputs[key]["d"] == r["selected"] != r["before"]
            assert key not in r["commit_active"] and not prior[r["selected"]]
            assert (
                sum(n == r["selected"] for n in r["commit_active"].values())
                < arm["batch_config"]["max_sequences"]
            )
            assert prior[r["before"]].count(key) == 1
            expected = [list(q) for q in prior]
            expected[r["before"]].remove(key)
            expected[r["selected"]].append(key)
            assert after == expected
            assert controls[key]["start"] >= r["resolved"] - 1e-8
        else:
            assert prior == after and outputs[key]["d"] == r["before"]
            if r["status"] == "acquired_or_completed":
                assert controls[key]["start"] <= r["resolved"]
    owned = {r["id"] for r in records}
    assert all(
        o["d"] == decisions[k]["initial_forecast"]["d"]
        for k, o in outputs.items()
        if k not in owned
    )
    close(
        arm["decode_advice_CPU_wall"],
        sum(r["CPU_wall"] + r["commit_CPU_wall"] for r in records),
    )
    return {
        "calls": len(records),
        "moves": sum(r["status"] == "move" for r in records),
        "CPU_wall": arm["decode_advice_CPU_wall"],
        "all_checks_passed": True,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    cases = []
    for i in (1, 2, 3):
        raw = json.loads((args.result / f"case-{i:02d}.json").read_bytes())
        cases.append({"rate": raw["rate"], **audit(raw["arms"]["ours"])})
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(
            {
                "CPU_only": True,
                "real_parallel_serving_validated": False,
                "cases": cases,
            },
            stream,
            indent=2,
        )


if __name__ == "__main__":
    main()
