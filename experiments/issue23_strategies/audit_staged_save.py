"""Audit full-work success and actual publication in the corrected CPU world."""

import argparse
import json
from collections import Counter
from pathlib import Path

import simulate_continuous_batch  # noqa: F401
from staged_save_model import VERSION


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--result", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    a = p.parse_args()
    scope = json.loads((a.result / "scope.json").read_bytes())
    assert scope["staged_save_world"] and not scope["zero_control_diagnostic"]
    rows = []
    for i in (1, 2, 3):
        case = json.loads((a.result / f"case-{i:02d}.json").read_bytes())
        row = {"rate": case["rate"], "arms": {}}
        for name, arm in case["arms"].items():
            assert arm["save_publication_model"]["version"] == VERSION
            actions = arm["actions"]
            assert not any(e["kind"] == "save_allocation_failed" for e in actions)
            publications = [e for e in actions if e["kind"] == "save_publication"]
            counts = Counter((e["request"], "P" if e["node"] < 2 else "D") for e in publications)
            assert len(publications) == len(counts) == 190 and set(counts.values()) == {1}
            assert len({e["request"] for e in publications}) == 95
            for e in publications:
                assert e["requested_blocks"] == e["published_new_blocks"] == len(e["new_keys"])
                assert e["peak_unique_bytes"] <= e["capacity_bytes"]
                assert e["at"] is not None and e["publication_model"] == VERSION
                assert len(e["new_owners"]) == len(set(e["new_owners"]))
            reclaims = [e for e in actions if e["kind"] == "forecast_native_reclaim"]
            for e in reclaims:
                assert e["at"] is not None and e["publication_model"] == VERSION
                assert e["logical_blocks"] == sum(len(v["keys"]) for v in e["victims"])
                assert e["charged"] == sum(v["charged"] for v in e["victims"] if not v["retained_by_lease"])
                assert e["retained_lease_bytes"] == sum(v["charged"] for v in e["victims"] if v["retained_by_lease"])
                prefix = f"staged:{e['node']}:{e['request']}:"
                assert not any(v["owner"].startswith(prefix) for v in e["victims"])
            if name != "native":
                assert len(arm["initial_cache_forecasts"]) == 95
                assert all(q["forecast_save_model"] == VERSION for q in arm["initial_cache_forecasts"].values())
            row["arms"][name] = {"publications": 190, "reclaims": len(reclaims),
                                  "lease_retained_eviction_events": sum(e["retained_lease_bytes"] > 0 for e in reclaims),
                                  "save_model_CPU_wall": arm["save_publication_model"]["save_model_CPU_wall"],
                                  "TTFT": arm["TTFT"]}
        row["same_world_p99_reduction_percent"] = 100 * (1 - case["arms"]["ours"]["TTFT"]["p99"] / case["arms"]["reference"]["TTFT"]["p99"])
        rows.append(row)
    with a.output.open("x", encoding="utf-8") as f:
        json.dump({"all_checks_passed": True, "CPU_only": True, "same_world_comparisons_only": True, "cases": rows}, f, indent=2)


if __name__ == "__main__":
    main()
