import hashlib
import json
from pathlib import Path

base = Path(__file__).resolve().parent


def summarize(path):
    screen = json.loads((path / "screen.json").read_text())
    audit = json.loads((path / "accounting-audit.json").read_text())
    result = {
        "directory": str(path),
        "screen_sha256": hashlib.sha256(
            (path / "screen.json").read_bytes()
        ).hexdigest(),
        "audit": audit["kind"],
        "validated_arms": audit["validated_arms"],
        "cases": [],
    }
    for case in screen["cases"]:
        item = {"rate": case["rate"], "arms": {}}
        for name, arm in case["arms"].items():
            s = arm.get("search_records", [])
            item["arms"][name] = {
                "TTFT": arm["TTFT"],
                "makespan": arm["makespan"],
                "planning_seconds": arm["planning_wall"],
                "boundary_seconds": arm["boundary_CPU_wall"],
                "planning_plus_boundary_seconds": arm["planning_wall"]
                + arm["boundary_CPU_wall"],
                "iteration_cpu_seconds": arm.get("iteration_scheduler_wall_charged"),
                "pruned_calendars": sum(r.get("pruned_calendars", 0) for r in s),
                "bound_checks": sum(r.get("bound_checks", 0) for r in s),
                "calendar_full": sum(r.get("calendar_full", 0) for r in s),
                "P_counts": [
                    sum(r["p"] == n for r in arm["per_request"]) for n in (0, 1)
                ],
                "D_counts": [
                    sum(r["d"] == n for r in arm["per_request"]) for n in (2, 3)
                ],
            }
        ref, new = (item["arms"][n] for n in ("reference", "ours"))
        item["net_p99_reduction_vs_reference_percent"] = 100 * (
            1 - new["TTFT"]["p99"] / ref["TTFT"]["p99"]
        )
        if ref["planning_plus_boundary_seconds"]:
            item["search_wall_reduction_percent"] = 100 * (
                1
                - new["planning_plus_boundary_seconds"]
                / ref["planning_plus_boundary_seconds"]
            )
        item["exact_mechanism_equivalence"] = case.get(
            "zero_control_exact_mechanism_equivalence"
        )
        result["cases"].append(item)
    return result


if __name__ == "__main__":
    import sys

    result = summarize(Path(sys.argv[1]))
    target = Path(sys.argv[2])
    target.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))
