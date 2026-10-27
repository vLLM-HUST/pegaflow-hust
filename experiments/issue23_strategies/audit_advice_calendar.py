"""Independently audit partial cancellation, completed cost and DAG provenance."""

import argparse
import json
from pathlib import Path

from audit_late_decode import close
from audit_yielding_decode import audit as audit_search


def audit(arm):
    summary = audit_search(arm)
    records = arm["decode_advice"]
    by_id = {r["id"]: r for r in records}
    quotes = arm["calendar_window_records"]
    for q in quotes:
        r = by_id[q["id"]]
        calendar = r["incumbent_calendar"]
        assert calendar["model_latest"] <= calendar["origin"] <= q["origin"]
        close(q["origin"] + q["CPU_wall"], q["ready"])
        assert q["CPU_wall"] >= 0
        known = [
            v
            for v in records
            if v is not r and v["completed_search"] and v["resolved"] <= q["origin"]
        ]
        assert q["history"] == [v["id"] for v in known]
        samples = [
            sum(t["CPU_wall"] for t in v["quanta"][1:]) + v["commit_CPU_wall"]
            for v in known
        ]
        assert q["remaining_CPU_samples"] == samples
        estimate = sum(samples) / len(samples) if samples else None
        assert q["predicted_remaining_CPU"] == estimate
        assert q["history_latest"] == max((v["resolved"] for v in known), default=None)
        acquired = calendar["D_acquire"]
        assert q["predicted_D_acquire"] == acquired
        for op in calendar["operations"]:
            if op["key"] == q["id"] and op["operation"] == "D_control":
                close(op["start"], acquired)
        assert q["cancel"] == (
            estimate is not None
            and acquired is not None
            and acquired <= q["origin"] + estimate
        )
        if q["cancel"] and q["existing_cancellation"] is None:
            assert r["status"] == "predicted_acquisition_before_search"
            assert not r["completed_search"] and len(r["quanta"]) == 1
            assert r["resolved"] >= q["ready"]
            assert r["commit_queue_before"] == r["commit_queue_after"]
    close(arm["calendar_window_CPU_wall"], sum(q["CPU_wall"] for q in quotes))
    return {
        **summary,
        "window_checks": len(quotes),
        "window_cancellations": sum(
            r["status"] == "predicted_acquisition_before_search" for r in records
        ),
        "trigger_CPU_wall": arm["calendar_window_CPU_wall"],
    }


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--result", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    a = p.parse_args()
    rows = []
    for i in (1, 2, 3):
        raw = json.loads((a.result / f"case-{i:02d}.json").read_bytes())
        assert (
            raw["arms"]["ours"]["internal_candidate"]["name"]
            == "advice-calendar-window-01"
        )
        rows.append({"rate": raw["rate"], **audit(raw["arms"]["ours"])})
    with a.output.open("x", encoding="utf-8") as f:
        json.dump(
            {
                "CPU_only": True,
                "cases": rows,
                "all_checks_passed": len(rows) == 3
                and all(row["all_checks_passed"] for row in rows),
            },
            f,
            indent=2,
        )


if __name__ == "__main__":
    main()
