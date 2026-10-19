"""Frozen synthetic offered arrivals, separate from causal session readiness."""

import argparse
import hashlib
import json
import math
from pathlib import Path
import random
import statistics


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def offsets(count, rate, cv, seed):
    if type(count) is not int or count < 1 or not math.isfinite(rate) or rate <= 0:
        raise ValueError("A finite positive request rate and request count are required")
    if not math.isfinite(cv) or cv <= 0 or type(seed) is not int:
        raise ValueError("A positive arrival CV and an integer seed are required")
    rng, result = random.Random(seed), [0.0]
    for _ in range(count - 1):
        interval = rng.expovariate(rate) if cv == 1 else rng.gammavariate(1 / cv**2, cv**2 / rate)
        result.append(result[-1] + interval)
    return result


def moments(times):
    gaps = [b - a for a, b in zip(times, times[1:])]
    mean = statistics.mean(gaps) if gaps else 0
    return {"requests": len(times), "span_seconds": times[-1] - times[0] if times else 0,
            "realized_rps": 1 / mean if mean > 0 else None,
            "realized_gap_cv": statistics.pstdev(gaps) / mean if mean > 0 else None}


def causal_ready(offered, previous_completion):
    return max(offered, previous_completion)


def load_condition(path, name, original_protocol):
    suite = json.loads(path.read_text(encoding="utf-8"))
    if (suite["request_ids"] != original_protocol["test_order"]
            or suite["requests_sha256"] != original_protocol["source_manifest"]["requests_sha256"]
            or suite["cost_sha256"] != original_protocol["cost_sha256"]
            or suite["calibration_only"] is not True):
        raise ValueError("Arrival manifest changed the frozen inputs or used test outcomes")
    if ("reference_protocol_sha256" in original_protocol
            and suite["source_protocol_sha256"] != original_protocol["reference_protocol_sha256"]):
        raise ValueError("Arrival reference protocol differs")
    matches = [case for case in suite["conditions"] if case["name"] == name]
    if len(matches) != 1:
        raise ValueError("Select exactly one predeclared arrival condition")
    condition = matches[0]
    times = condition["offered_arrivals_seconds"]
    if (len(times) != len(suite["request_ids"]) or not times or times[0] != 0
            or any(isinstance(t, bool) or not isinstance(t, (int, float)) or not math.isfinite(t) or t < 0 for t in times)
            or any(a >= b for a, b in zip(times, times[1:]))):
        raise ValueError("Invalid frozen arrival ledger")
    if condition["distribution"] not in ("poisson", "gamma"):
        raise ValueError("Undeclared arrival distribution")
    realized_hash = hashlib.sha256(json.dumps(times, separators=(",", ":")).encode()).hexdigest()
    if realized_hash != condition["offered_arrivals_sha256"]:
        raise ValueError("Changed frozen arrival realization")
    return suite, condition


def build(reference, output, seed=20261014):
    protocol = json.loads((reference / "protocol.json").read_text(encoding="utf-8"))
    if sha(reference / "cost-model.json") != protocol["cost_sha256"]:
        raise ValueError("Original calibration cost changed")
    paths = sorted((reference / "calibration").glob("calibration-*.json"))
    records = [json.loads(path.read_text(encoding="utf-8")) for path in paths]
    if len(records) != protocol["source_manifest"]["calibration"]["requests"]:
        raise ValueError("Incomplete original calibration; no test-based rate tuning")
    handoffs = [row["handoff"]["seconds"] for row in records if "seconds" in row["handoff"]]
    if not handoffs:
        raise ValueError("No observed handoff durations; do not price an unmeasured link")
    means = {"P_service": statistics.mean(row["P_wire"]["complete_seconds"] for row in records),
             "D_service": statistics.mean(row["D_wire"]["complete_seconds"] for row in records),
             "PD_handoff": statistics.mean(handoffs)}
    capacity = {"P": 2 / means["P_service"], "D": 2 / means["D_service"],
                "serialized_PD_handoff": 1 / means["PD_handoff"]}
    reference_rate = min(capacity.values())
    conditions = []
    for name, fraction, cv in (("poisson-050", 0.5, 1), ("poisson-075", 0.75, 1),
                               ("poisson-100", 1.0, 1), ("gamma-cv2-100", 1.0, 2)):
        case_seed = seed if cv == 1 else seed + 1
        rate = reference_rate * fraction
        times = offsets(len(protocol["test_order"]), rate, cv, case_seed)
        conditions.append({"name": name, "distribution": "poisson" if cv == 1 else "gamma",
                           "load_fraction_of_reference": fraction, "rate_rps": rate,
                           "interarrival_cv": cv, "seed": case_seed,
                           "gamma_shape": 1 / cv**2, "gamma_scale_seconds": cv**2 / rate,
                           "offered_arrivals_seconds": times,
                           "offered_arrivals_sha256": hashlib.sha256(json.dumps(times, separators=(",", ":")).encode()).hexdigest(),
                           "realization": moments(times)})
    suite = {"schema": "issue23-causal-arrivals-v1", "calibration_only": True,
             "source_protocol_sha256": sha(reference / "protocol.json"),
             "cost_sha256": protocol["cost_sha256"],
             "requests_sha256": protocol["source_manifest"]["requests_sha256"],
             "request_ids": protocol["test_order"], "reference_rate_rps": reference_rate,
             "reference_rate_is_capacity_estimate_not_measured_saturation": True,
             "reference_calibration_stage_means_seconds": means,
             "handoff_duration_observations": len(handoffs),
             "handoff_duration_missing_not_imputed_zero": len(records) - len(handoffs),
             "handoff_capacity_uses_conditional_observed_durations": True,
             "reference_capacity_estimates_rps": capacity,
             "calibration_ledgers": {path.name: sha(path) for path in paths},
             "conditions": conditions, "execution_order": ["poisson-075", "gamma-cv2-100", "poisson-050", "poisson-100"],
             "first_arrival_conditioned_at_zero": True,
             "same_nominal_offered_times_in_both_arms": True,
             "causal_ready_rule": "max(offered_arrival, previous_same_session_full_completion)",
             "external_think_time_seconds": 0,
             "TTFT_origin": "causal_ready_before_router_queue",
             "upstream_causality_wait_reported_separately": True,
             "actual_ingress_not_claimed_to_remain_Poisson_or_identical_across_arms": True,
             "dataset_step_timestamps_used_as_request_submit_times": False,
             "sample_scope": "fixed same-input synthetic stress replay; not production trace or stable-p99",
             "sources": ["https://www.usenix.org/system/files/osdi24-zhong-yinmin.pdf",
                         "https://arxiv.org/html/2407.00023v2",
                         "https://www.usenix.org/system/files/osdi24-sun-biao.pdf"]}
    with output.open("x", encoding="utf-8") as stream:
        json.dump(suite, stream, indent=2)
        stream.write("\n")
    print(json.dumps({"output": str(output), "sha256": sha(output), "reference_rate_rps": reference_rate,
                      "conditions": [{key: value for key, value in case.items() if key != "offered_arrivals_seconds"}
                                     for case in conditions]}))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    build(args.reference, args.output)


if __name__ == "__main__":
    main()
