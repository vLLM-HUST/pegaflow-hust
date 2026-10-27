"""Independent ownership, completed-history and charged-delay checks for late D."""

import argparse
import json
import math
from pathlib import Path


def close(a, b):
    assert math.isclose(a, b, rel_tol=1e-12, abs_tol=1e-8), (a, b)


def audit(arm):
    records = arm.get("late_decode_decisions", [])
    assert len({r["id"] for r in records}) == len(records)
    stages = {(r["id"], r["node"] < 2): r for r in arm["stages"]}
    decisions = {r["id"]: r["initial_forecast"] for r in arm["decisions"]}
    outputs = {r["id"]: r for r in arm["per_request"]}
    controls = {
        r["request"]: r for r in arm["resource_events"] if r["operation"] == "D_control"
    }
    owned = {r["id"]: r for r in records}
    for r in records:
        key = r["id"]
        known = [
            o for o in records if o["decision_ready"] <= r["origin"] and o is not r
        ]
        assert r["completed_CPU_samples"] == len(known)
        close(
            r["predicted_CPU"],
            sum(o["CPU_wall"] for o in known) / len(known) if known else 0,
        )
        assert r["CPU_latest"] == max(
            (o["decision_ready"] for o in known), default=None
        )
        assert r["model_latest"] <= r["origin"]
        close(stages[key, True]["completed"], r["origin"])
        close(r["decision_ready"], r["origin"] + r["CPU_wall"] + r["RPC_seconds"])
        assert r["committed"] >= r["decision_ready"] + r["commit_CPU_wall"] - 1e-8
        assert controls[key]["start"] >= r["committed"] - 1e-8
        assert r["before"] == decisions[key]["d"]
        assert r["after"] == outputs[key]["d"] == stages[key, False]["node"]
        assert outputs[key]["p"] == decisions[key]["p"] == stages[key, True]["node"]
        assert (
            r["slots"][str(r["before"])] >= arm["batch_config"]["max_sequences"]
            or r["queued"][str(r["before"])]
        )
        other = str(5 - r["before"])
        assert (
            r["slots"][other] < arm["batch_config"]["max_sequences"]
            and r["queued"][other] == 0
        )
        assert r["evaluations"]
    for key, value in outputs.items():
        if key not in owned:
            assert value["d"] == decisions[key]["d"]
    close(
        arm["late_decode_CPU_wall"],
        sum(r["CPU_wall"] + r["commit_CPU_wall"] for r in records),
    )
    return {
        "decisions": len(records),
        "changed_destination": sum(r["before"] != r["after"] for r in records),
        "CPU_wall": arm["late_decode_CPU_wall"],
        "RPC_wall": sum(r["RPC_seconds"] for r in records),
        "source_and_state_checks_passed": True,
        "no_free_or_future_decision_labels": True,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    cases = []
    for i in (1, 2, 3):
        case = json.loads((args.result / f"case-{i:02d}.json").read_bytes())
        cases.append({"rate": case["rate"], **audit(case["arms"]["ours"])})
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
