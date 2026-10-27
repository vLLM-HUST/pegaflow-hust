"""Audit actual phase-order activation and unchanged full work accounting."""

import argparse
import json
from pathlib import Path

from audit_yielding_decode import audit as audit_advice


def audit(arm):
    assert arm["internal_candidate"]["name"] == "ready-prefill-before-restore-01"
    assert len(arm["initial_cache_forecasts"]) == 95
    assert all(
        q["forecast_batch_factory"] == "ReadyPrefillBatcher"
        for q in arm["initial_cache_forecasts"].values()
    )
    records = arm["restore_inversions"]
    for r in records:
        older = []
        for key, phase in r["eligible_phases"]:
            if phase in ("restore", "save"):
                assert phase == "restore" and key == r["deferred_restore"] and older
                break
            if phase == "prefill":
                older.append(key)
        assert list(r["older_prefill"]) == older
        assert all(
            key in older and count > 0 and phase == "prefill"
            for key, count, phase in r["selected_pieces"]
        )
        iterations = [
            e
            for e in arm["batch_iterations"]
            if e["node"] == r["node"] and e["start"] >= r["origin"] - 1e-8
        ]
        assert iterations
        actual = min(iterations, key=lambda e: e["start"])
        assert actual["kind"] == "compute"
        # Coalescing may multiply token counts, but must not change members/phases.
        assert [(k, p) for k, n, p in actual["pieces"]] == [
            (k, p) for k, n, p in r["selected_pieces"]
        ]
    advice = audit_advice(arm)
    return {
        "all_checks_passed": True,
        "phase_inversions_avoided": len(records),
        "advice": advice,
        "forecast_execution_rule_matches": True,
    }


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--result", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    a = p.parse_args()
    rows = []
    for i in (1, 2, 3):
        raw = json.loads((a.result / f"case-{i:02d}.json").read_bytes())
        rows.append({"rate": raw["rate"], **audit(raw["arms"]["ours"])})
    with a.output.open("x", encoding="utf-8") as f:
        json.dump(
            {
                "CPU_only": True,
                "all_checks_passed": all(r["all_checks_passed"] for r in rows),
                "cases": rows,
            },
            f,
            indent=2,
        )


if __name__ == "__main__":
    main()
