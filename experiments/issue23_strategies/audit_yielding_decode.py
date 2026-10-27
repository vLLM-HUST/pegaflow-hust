"""Independent completed-history, cooperative control and owned-commit checks."""

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
    for a, b in itertools.pairwise(records):
        assert a["resolved"] <= b["origin"] + 1e-8
    for r in records:
        key, origin = r["id"], r["origin"]
        forecast_origin = r["forecast_origin"]
        close(forecast_origin, origin + r["setup_CPU_wall"])
        known = [
            v
            for v in records
            if v is not r
            and v["completed_search"]
            and v["decision_ready"] <= forecast_origin
        ]
        close(
            r["predicted_CPU"],
            sum(v["CPU_wall"] for v in known) / len(known) if known else 0,
        )
        assert r["completed_CPU_samples"] == len(known)
        assert r["CPU_latest"] == max(
            (v["decision_ready"] for v in known), default=None
        )
        assert r["model_latest"] <= forecast_origin
        close(r["P_completed"], stages[key, True]["completed"])
        assert r["P_completed"] <= origin
        assert r["quanta"] and len(r["quanta"]) <= 65
        close(r["quanta"][0]["origin"], origin + r["setup_CPU_wall"])
        close(
            r["CPU_wall"], r["setup_CPU_wall"] + sum(q["CPU_wall"] for q in r["quanta"])
        )
        close(r["RPC_seconds"], sum(q["RPC_seconds"] for q in r["quanta"]))
        assert all(q["RPC_seconds"] == 0 for q in r["quanta"][1:])
        for q in r["quanta"]:
            close(q["ready"], q["origin"] + q["CPU_wall"] + q["RPC_seconds"])
            assert q["origin"] <= controls[key]["start"] + 1e-8
        for a, b in itertools.pairwise(r["quanta"]):
            assert a["ready"] <= b["origin"] + 1e-8
        close(r["decision_ready"], r["quanta"][-1]["ready"])
        assert r["completed_search"] == (r["quanta"][-1]["kind"] == "done")
        assert r["resolved"] >= r["decision_ready"] + r["commit_CPU_wall"] - 1e-8
        assert arm["makespan"] >= r["resolved"]
        assert all(
            not origin <= d["origin"] < r["resolved"] - 1e-8 for d in decisions.values()
        )
        prior, after = r["commit_queue_before"], r["commit_queue_after"]
        initial = decisions[key]["initial_forecast"]
        assert initial["d"] == r["before"]
        assert initial["p"] == outputs[key]["p"] == stages[key, True]["node"]
        keep, *options = r["evaluations"]
        assert keep["kind"] == "keep" and keep["delay"] == 0
        assert len(r["evaluations"]) <= 64
        if r["status"] == "move":
            assert r["completed_search"]
            assert outputs[key]["d"] == r["selected"] != r["before"]
            assert any(
                v["objective"] < keep["objective"] and v["d"] == r["selected"]
                for v in options
            )
            assert key not in r["commit_active"] and not prior[r["selected"]]
            assert (
                sum(n == r["selected"] for n in r["commit_active"].values())
                < arm["batch_config"]["max_sequences"]
            )
            expected = [list(q) for q in prior]
            expected[r["before"]].remove(key)
            expected[r["selected"]].append(key)
            assert after == expected and controls[key]["start"] >= r["resolved"] - 1e-8
        else:
            assert prior == after and outputs[key]["d"] == r["before"]
            if r["status"] == "acquired_or_completed":
                assert controls[key]["start"] <= r["resolved"]
            if r["status"] == "route_priority":
                assert not r["completed_search"]
    close(
        arm["decode_advice_CPU_wall"],
        sum(r["CPU_wall"] + r["commit_CPU_wall"] for r in records),
    )
    return {
        "calls": len(records),
        "moves": sum(r["status"] == "move" for r in records),
        "partial_searches": sum(not r["completed_search"] for r in records),
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
