"""Verify a fixed-state CPU profile against its untouched causal parent replay."""

import argparse
import hashlib
import json
from collections import defaultdict
from pathlib import Path


def analyze(result, zero):
    profile = json.loads((result / "paired-profile.json").read_bytes())
    actual = json.loads((result / "fixed-control-result.json").read_bytes())
    reference = json.loads((zero / "case-02.json").read_bytes())["arms"]["reference"]
    scope = json.loads((zero / "scope.json").read_bytes())
    assert profile["input_sha256"] == scope["input_sha256"]
    for field in ("per_request", "stages", "resource_events", "batch_iterations"):
        assert actual[field] == reference[field], field
    assert actual["requests"] == 95 and profile["measurements"]
    summary = {}
    for kind in ("route", "boundary"):
        rows = [r for r in profile["measurements"] if r["kind"] == kind]
        assert rows and all(r["equal"] for r in rows)
        values = {}
        for name in ("parent", "delta"):
            measurements = [
                m
                for row in rows
                for m in row["measurements"]
                if m["implementation"] == name
            ]
            assert len(measurements) == 2 * len(rows)
            functions = defaultdict(
                lambda: {"calls": 0, "self_seconds": 0.0, "cumulative_seconds": 0.0}
            )
            for row in rows:
                for f in row["profiles"][name]["functions"]:
                    target = functions[
                        Path(f["file"]).name
                        + ":"
                        + str(f["line"])
                        + ":"
                        + f["function"]
                    ]
                    for key in target:
                        target[key] += f[key]
            values[name] = {
                "unprofiled_CPU_sum_two_repeats": sum(r["cpu"] for r in measurements),
                "unprofiled_wall_sum_two_repeats": sum(r["wall"] for r in measurements),
                "clone_CPU_excluded": sum(
                    r["clone_cpu_excluded"] for r in measurements
                ),
                "profiled_functions_nested_not_additive": dict(
                    sorted(functions.items(), key=lambda item: -item[1]["self_seconds"])
                ),
            }
        old, new = (
            values[name]["unprofiled_CPU_sum_two_repeats"]
            for name in ("parent", "delta")
        )
        summary[kind] = {
            "calendars": len(rows),
            "implementations": values,
            "CPU_reduction_percent": 100 * (1 - new / old),
        }
    return {
        "CPU_only": True,
        "net_performance_claim": False,
        "causal_replay_unchanged": True,
        "equal_cloned_and_original_calendar_predictions": True,
        "input_sha256": profile["input_sha256"],
        "raw_profile_sha256": hashlib.sha256(
            (result / "paired-profile.json").read_bytes()
        ).hexdigest(),
        "summary": summary,
        "limitations": "Two requests in a zero-control middle-rate replay; paired-state implementation timing only, not all states or an end-to-end speedup. Profiler totals are separate and nested.",
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--zero", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(analyze(args.result, args.zero), stream, indent=2)
