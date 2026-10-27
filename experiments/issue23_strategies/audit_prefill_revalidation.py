"""Audit unacquired P choice, charged FIFO wait and the single control lane."""

import argparse
import itertools
import json
from pathlib import Path

from audit_late_decode import close
from audit_yielding_decode import audit as audit_D


def audit(arm):
    d = audit_D(arm)
    records = arm["prefill_revalidation"]
    starts = [r for r in arm["resource_operation_ledger"] if r["event"] == "start"]
    initial = {r["id"]: r for r in arm["decisions"]}
    outputs = {r["id"]: r for r in arm["per_request"]}
    intervals = [(r["origin"], r["resolved"], "P") for r in records]
    intervals += [(r["origin"], r["resolved"], "D") for r in arm["decode_advice"]]
    intervals += [
        (r["origin"], o["completed"], "route")
        for r, o in zip(
            arm["decisions"], arm["control_work_observations"]["route"], strict=True
        )
    ]
    assert all(a[1] <= b[0] + 1e-8 for a, b in itertools.pairwise(sorted(intervals)))
    assert len({r["id"] for r in records}) == len(records)
    for r in records:
        key, node = r["id"], r["node"]
        known = [v for v in records if v["resolved"] <= r["origin"] and v is not r]
        close(
            r["predicted_CPU"],
            sum(v["CPU_wall"] + v["commit_CPU_wall"] for v in known) / len(known)
            if known
            else 0.0,
        )
        assert r["completed_CPU_samples"] == len(known)
        assert r["CPU_latest"] == max((v["resolved"] for v in known), default=None)
        assert r["model_latest"] <= r["origin"]
        close(r["decision_ready"], r["origin"] + r["CPU_wall"] + r["RPC_seconds"])
        assert r["resolved"] >= r["decision_ready"] + r["commit_CPU_wall"] - 1e-8
        assert r["queue_before"][0] == key and r["queue_after"][0] == key
        # Other routes cannot enqueue during this serialized decision. Requests
        # already ahead cannot exist because the decision owns the FIFO head.
        assert r["queue_before"] == r["queue_after"]
        assert initial[key]["initial_forecast"]["p"] == node == outputs[key]["p"]
        assert r["source"] < 2 and r["source"] != node
        source_key, source_name = r["source_owner"]
        assert source_name == "P_api"
        source_start = next(
            e
            for e in starts
            if e["key"] == source_key and e["operation"] == source_name
        )
        source_finish = next(
            e
            for e in arm["resource_operation_ledger"]
            if e["key"] == source_key
            and e["operation"] == source_name
            and e["event"] == "finish"
        )
        assert source_start["seconds"] <= r["origin"] < source_finish["seconds"]
        own = [
            e
            for e in starts
            if e["key"] == key and e["operation"] in ("history", "P_api")
        ]
        assert own and min(e["seconds"] for e in own) >= r["resolved"] - 1e-8
        keep, *options = r["evaluations"]
        assert (
            keep["kind"] == "keep" and keep["delay"] == 0 and len(r["evaluations"]) <= 3
        )
        if r["status"] == "local":
            assert r["selected_source"] == node
            assert any(
                v["prefix"] == r["selected_prefix"]
                and v["objective"] < keep["objective"]
                for v in options
            )
            assert not any(e["operation"] == "history" for e in own)
        else:
            assert r["selected_source"] == r["source"]
            assert r["selected_prefix"] == r["initial_prefix"]
        assert arm["makespan"] >= r["resolved"]
    close(
        arm["prefill_revalidation_CPU_wall"],
        sum(r["CPU_wall"] + r["commit_CPU_wall"] for r in records),
    )
    return {
        "calls": len(records),
        "changed": sum(r["status"] == "local" for r in records),
        "CPU_wall": arm["prefill_revalidation_CPU_wall"],
        "D_advice": d,
        "all_checks_passed": True,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    rows = []
    for i in (1, 2, 3):
        raw = json.loads((args.result / f"case-{i:02d}.json").read_bytes())
        rows.append(
            {
                "rate": raw["rate"],
                "reference_D_advice": audit_D(raw["arms"]["reference"]),
                **audit(raw["arms"]["ours"]),
            }
        )
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump({"CPU_only": True, "cases": rows}, stream, indent=2)
    print(json.dumps(rows))


if __name__ == "__main__":
    main()
