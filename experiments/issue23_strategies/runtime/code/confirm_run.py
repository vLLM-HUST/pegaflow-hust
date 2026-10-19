"""Same-allocation B/T/B performance bracket using the frozen resource core."""

from __future__ import annotations

import argparse
import contextlib
import importlib
import json
from pathlib import Path
import signal
import sys
import time

from common import AGENT_PORT, D_IP, P_IP, ROOT, META_GRPC, META_HTTP, assert_port_free, dump, file_sha, rpc
from confirmation_control import ARMS, SERVING_PHASE
from confirmation_state import allocation_identity, require_same, state_digest
from confirm_node import PORTS, source_identity
import run as core


CORE_SHA = file_sha(Path(core.__file__))


def copy_seed(source, destination):
    files = sorted(source.glob("calibration-*.json"))
    if len(files) != 32:
        raise ValueError("Original 32 calibration files required, not bracket measurements")
    destination.mkdir(exist_ok=False)
    for path in files:
        with (destination / path.name).open("xb") as stream:
            stream.write(path.read_bytes())
    return {path.name: file_sha(path) for path in sorted(destination.glob("*.json"))}


def prepare_protocol(args, manifest, test):
    seed = copy_seed(args.operation_seed, args.output / "operation-seed")
    dump(args.output / "operation-seed-manifest.json", seed)
    costs, interval = core.load_pipeline_reference(args.pipeline_reference, manifest, test, args.output,
                                                   args.arrival_manifest, "poisson-300")
    reference = json.loads((args.output / "protocol.json").read_text(encoding="utf-8"))
    template = json.loads(args.candidate_protocol.read_text(encoding="utf-8"))
    for key in ("source_manifest", "test_order", "cost_sha256", "arrival_condition",
                "model", "tp", "P", "D", "dtype", "block", "prefill", "context",
                "pool_bytes_per_instance", "npu_kv_bytes", "P_output", "D_output"):
        require_same(reference[key], template[key], "frozen protocol " + key)
    if (template["candidate_revision"] != "phase-release-live-01"
            or template["controller_core_sha256"] != CORE_SHA):
        raise ValueError("Confirmation must reuse the exact frozen detached calendar core")
    require_same(template["operation_seed_manifest_sha256"],
                 file_sha(args.output / "operation-seed-manifest.json"), "original seed manifest")
    executed = {}
    for name, expected in template["policy_source_sha256"].items():
        source = Path(importlib.import_module("pegaflow." + name).__file__)
        require_same(expected, file_sha(source), "candidate module " + name)
        executed[name] = {"path": str(source), "sha256": expected}
        with (args.output / (name + "-executed-source.py")).open("xb") as stream:
            stream.write(source.read_bytes())
    template.update(confirmation_arms=ARMS, confirmation_serving_phase=SERVING_PHASE,
                    confirmation_scope="one temporal B/T/B bracket; not randomized causal isolation",
                    actual_initial_cache_equality_required=True,
                    whole_bracket_same_allocation_required=True,
                    confirmation_source_manifest_sha256=file_sha(ROOT / "confirmation-sources.json"))
    core.save_candidate_protocol(args.output, template)
    dump(args.output / "event-queue-contract.json", {
        "candidate_revision": "phase-release-live-01", "cost_sha256": template["cost_sha256"],
        "executed_policy_sources": executed, "cost_coefficients_refitted": False,
        "additional_operation_model_seed": "original32calibration-only",
        "additional_operation_model_online_update": "past-completed-only",
        "test_outcome_tuned_thresholds": False, "algorithm_revised_after_exposed_results": True})
    return costs, interval, template, seed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--pipeline-reference", type=Path, required=True)
    parser.add_argument("--candidate-protocol", type=Path, required=True)
    parser.add_argument("--operation-seed", type=Path, required=True)
    parser.add_argument("--arrival-manifest", type=Path, required=True)
    args = parser.parse_args()
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    args.output.mkdir(parents=True, exist_ok=True)
    if (args.output / "confirmation-contract.json").exists():
        raise ValueError("Refusing to reuse or resume an existing bracket")
    acquired = False
    try:
        sources = source_identity()
        d_state = rpc(f"http://{D_IP}:{AGENT_PORT}/status", timeout=5)
        if (d_state.get("ready") is not True or d_state.get("cursor") != 0 or
                d_state.get("phase") is not None or d_state.get("children") != [] or
                d_state.get("root") != str(ROOT)):
            raise ValueError("D is not a fresh owned confirmation controller")
        require_same(sources, d_state["confirmation_sources"], "P/D shared control sources")
        acquired = True
        initial_identity = {"P": {"allocation": allocation_identity(), "sources": sources},
                            "D": rpc(f"http://{D_IP}:{AGENT_PORT}/identity", timeout=30)}
        dump(args.output / "identity-initial.json", initial_identity)
        manifest = json.loads((ROOT / "workload/manifest.json").read_text(encoding="utf-8"))
        require_same(manifest["requests_sha256"], file_sha(ROOT / "workload/requests.jsonl"), "workload")
        rows = [json.loads(line) for line in (ROOT / "workload/requests.jsonl").read_text(encoding="utf-8").splitlines()]
        calibration = core.causal_order(rows, manifest["calibration_tasks"])
        test = core.causal_order(rows, manifest["test_tasks"])
        if len(test) != 95 or len({r["task"] for r in test}) != 8:
            raise ValueError("Confirmation must keep the declared 95/8 causal replay")
        warmup_rows = [next(r for r in calibration if r["task"] == task) for task in manifest["calibration_tasks"]]
        costs, interval, protocol, seed = prepare_protocol(args, manifest, test)
        dump(args.output / "confirmation-contract.json", {
            "arms": ARMS, "serving_phase": SERVING_PHASE,
            "no_test_data_used_as_initial_seed": True, "frozen_core_sha256": CORE_SHA,
            "source_manifest": sources, "first_token_outputs_compared_exactly": True,
            "prewarmup_empty_and_postwarmup_strict_state_required": True,
            "all_planning_migration_and_final_drain_costs_retained": True})
        snapshot = args.output / "executed-control"
        for name, expected in sources.items():
            target = snapshot / name
            target.parent.mkdir(parents=True, exist_ok=True)
            with target.open("xb") as stream:
                stream.write((ROOT / name).read_bytes())
            require_same(expected, file_sha(target), "executed snapshot")
        first_state, before_outputs, first_warmup = None, None, None

        def check_identity(label):
            actual = {"P": {"allocation": allocation_identity(), "sources": source_identity()},
                      "D": rpc(f"http://{D_IP}:{AGENT_PORT}/identity", timeout=30)}
            dump(args.output / ("identity-" + label + ".json"), actual)
            require_same(initial_identity, actual, "whole-bracket allocation/shared sources")

        for arm, policy_phase in ARMS:
            check_identity(arm + "-before")
            arm_root = args.output / arm
            arm_root.mkdir(exist_ok=False)
            require_same(seed, copy_seed(args.operation_seed, arm_root / "operation-seed"), "arm seed")
            directory, backend, workers, meta, log = core.services(SERVING_PHASE, arm_root)
            try:
                empty = [backend.inventory(n) for n in range(4)]
                dump(directory / "prewarmup-inventory.json", empty)
                state_digest(empty, empty=True)
                dump(directory / "counters-before.json", [backend.rdma_bytes(n) for n in range(4)])
                warmup = core.warmups(backend, warmup_rows, directory)
                warmup_tokens = [r["cold"]["output_token_ids"] for r in warmup]
                if first_warmup is None:
                    first_warmup = warmup_tokens
                require_same(first_warmup, warmup_tokens, "exact warmup outputs")
                inventory = [backend.inventory(n) for n in range(4)]
                dump(directory / "initial-inventory.json", inventory)
                digest = state_digest(inventory)
                if first_state is None:
                    first_state = digest
                dump(directory / "initial-state-contract.json", {"digest": digest, "reference": first_state,
                     "empty_before_warmup_passed": True, "serving_phase": SERVING_PHASE, "policy_phase": policy_phase})
                require_same(first_state, digest, "actual initial cache topology/content/rounded allocation")
                dump(directory / "formal-arm-started.json", {"phase": policy_phase, "confirmation_arm": arm,
                     "serving_phase": SERVING_PHASE, "model_variant": "phase" if arm == "T-phase" else "parent", "started_unix": time.time(), "four_ready": True,
                     "protocol_sha256": file_sha(args.output / "protocol.json")})
                result = core.formal(policy_phase, backend, test, costs, interval, directory,
                    pipeline=True, arrival_offsets=protocol["arrival_condition"]["offered_arrivals_seconds"],
                    causal_arrivals=True, event_queue_policy=True, baseline_routing="task-affinity",
                    slow_policy="off", operation_variant="phase" if arm == "T-phase" else "parent")
                outputs = {r["id"]: r["D"]["output_token_ids"] for r in result}
                if before_outputs is None:
                    before_outputs = outputs
                require_same(before_outputs, outputs, "all three arms exact 95x128 outputs")
                dump(directory / "counters-after.json", [backend.rdma_bytes(n) for n in range(4)])
                dump(directory / "final-inventory.json", [backend.inventory(n) for n in range(4)])
            finally:
                backend.close()
                core.close_services(SERVING_PHASE, workers, meta, log)
            assert_port_free(P_IP, (*PORTS, META_GRPC, META_HTTP))
            check_identity(arm + "-after")
        check_identity("final")
        rpc(f"http://{D_IP}:{AGENT_PORT}/finish", {}, timeout=120)
        dump(args.output / "pair-complete.json", {"completed": True, "arms": ARMS,
             "independent_audit_pending": True})
    except BaseException as error:
        dump(args.output / "failure.json", {"type": type(error).__name__, "error": str(error),
             "confirmation_valid": False, "automatic_retry_permitted": False})
        if acquired:
            with contextlib.suppress(OSError):
                rpc(f"http://{D_IP}:{AGENT_PORT}/abort", {}, timeout=120)
        raise


if __name__ == "__main__":
    main()
