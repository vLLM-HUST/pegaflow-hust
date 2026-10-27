"""Independently reconcile joint-phase labels and exclusive-gate accounting."""

import argparse
import hashlib
import json
from pathlib import Path

import simulate_continuous_batch as sim

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--result", type=Path, required=True)
parser.add_argument("--inputs", type=Path, required=True)
parser.add_argument("--seed", type=Path, required=True)
parser.add_argument("--output", type=Path, required=True)
args = parser.parse_args()
data = json.loads(args.inputs.read_bytes())
values = dict(data["costs"])
values["service"] = tuple(
    sim.parent.core.ObservedLatencyModel(**r) for r in values["service"]
)
values["decode_seconds"] = tuple(values["decode_seconds"])
model = sim.BatchSimulation(
    data["rows"],
    data["initial_inventory"],
    sim.Costs(**values),
    args.seed,
    data["control_cost_rows"],
    0.49032341538142143,
    "native",
    True,
    batch_config=sim.BatchConfig(),
)
report = {
    "scope": "synthetic phase placement; no hardware or pure-DMA validation",
    "arms": [],
}
for index in (1, 2, 3):
    for name in ("native", "reference", "ours"):
        arm = json.loads((args.result / f"case-{index:02d}-{name}.json").read_bytes())
        assert arm["phase_residual_world"] is True
        records = arm["phase_residual_samples"]
        assert len(records) == len(arm["stages"]) == 190
        assert len({(r["request"], r["node"]) for r in records}) == 190
        shifted_restore = shifted_save = 0.0
        for r in records:
            key, node, prefix = r["request"], r["node"], r["prefix"]
            first, tail = model.physics(key, node, model.rowmap[key]["tokens"], prefix)
            assert abs(first - r["first_total"]) < 1e-8
            assert abs(tail - r["tail_total"]) < 1e-8
            assert abs(r["restore_path"] + r["remaining_first_path"] - first) < 1e-8
            assert abs(r["finish_path"] + r["decode_path"] - tail) < 1e-8
            assert (
                min(
                    r[k]
                    for k in (
                        "restore_path",
                        "remaining_first_path",
                        "finish_path",
                        "decode_path",
                    )
                )
                >= 0
            )
            rows = [
                (v, w)
                for n, v, w in model.world_seed
                if n == node and bool(v["num_cached_tokens"]) == bool(prefix)
            ]
            old_restore = min(first, model.world.restore(node, prefix)) if prefix else 0
            old_save = min(tail, model.world.fits[node][4]) if node >= 2 else 0
            expected_restore, expected_save = old_restore, old_save
            if rows:
                chosen = int.from_bytes(
                    hashlib.sha256((key + ":" + str(node)).encode()).digest()[:8], "big"
                ) % len(rows)
                sample, wire = rows[chosen]
                assert r["calibration_id"] == sample["id"]
                raw = sample["raw_metrics"]
                if prefix:
                    expected_restore = min(
                        first,
                        max(
                            0,
                            model.world.restore(node, prefix)
                            + raw["scheduled_ts"]
                            - raw["queued_ts"]
                            - model.world.restore(node, sample["num_cached_tokens"]),
                        ),
                    )
                if node >= 2:
                    expected_save = min(
                        tail,
                        max(
                            0,
                            wire["complete_seconds"]
                            - wire["first_seconds"]
                            - (raw["last_token_ts"] - raw["first_token_ts"]),
                        ),
                    )
            assert abs(r["restore_path"] - expected_restore) < 1e-8
            assert abs(r["finish_path"] - expected_save) < 1e-8
            if node >= 2:
                shifted_restore += expected_restore - old_restore
                shifted_save += expected_save - old_save
        for kind, field in (("restore", "restore_path"), ("save", "finish_path")):
            elapsed = sum(
                e["end"] - e["start"]
                for e in arm["batch_iterations"]
                if e["kind"] == kind
            )
            cpu = sum(
                e["seconds"]
                for e in arm["iteration_scheduler_CPU"]
                if e["kind"] == kind
            )
            service = sum(r[field] for r in records if r["node"] >= 2)
            assert abs(elapsed - cpu - service) < 1e-6, (
                index,
                name,
                kind,
                elapsed,
                cpu,
                service,
            )
        report["arms"].append(
            {
                "rate_index": index,
                "arm": name,
                "phase_rows": len(records),
                "D_restore_shift_from_aggregate_at_same_states": shifted_restore,
                "D_finish_shift_from_aggregate_at_same_states": shifted_save,
                "aggregate_service_preserved": True,
                "exclusive_trace_reconciled": True,
            }
        )
with args.output.open("x", encoding="utf8") as stream:
    json.dump(report, stream, indent=2)
print(json.dumps(report, indent=2))
