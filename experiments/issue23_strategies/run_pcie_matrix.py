"""Reproduce a fixed three-rate CPU matrix with explicit evidence paths.

No GPU services, model loading, paper-specific hardware emulation or timers.
"""

import argparse
import hashlib
import json
import math
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict
from pathlib import Path


def write(path, value):
    if path.exists():
        raise FileExistsError(path)
    temporary = path.with_suffix(path.suffix + ".writing")
    with temporary.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.rename(path)


def cell(index, rate, options):
    if options.get("zero_control_diagnostic") or options.get("fixed_control_diagnostic"):
        # Mechanism equivalence only; never report this as a net performance run.
        time.perf_counter = lambda: 0.0
    repo = Path(options["repo"])
    sys.path.insert(0, str(repo / "python"))
    sys.path.insert(0, str(repo / "experiments/issue23_strategies"))
    import simulate_continuous_batch as sim
    from acquired_phase_search import AcquiredPhaseSimulation
    from advice_calendar_search import AdviceCalendarSimulation
    from advice_window_search import AdviceWindowSimulation
    from bounded_search import BoundedSimulation, ProofMemoSimulation
    from cache_evolving_search import CacheEvolvingSimulation
    from causal_prefill_search import CausalSimulation, LaxitySimulation
    from completed_phase_search import CompletedPhaseSimulation
    from control_release_search import ControlReleaseSimulation
    from control_work_search import ControlWorkSimulation
    from delta_protection_search import DeltaProtectionSimulation
    from dirty_scan_search import DirtyScanSimulation
    from fair_endpoint_search import FairLaxitySimulation, FairMemoSimulation
    from joint_acquire_search import JointAcquireSimulation
    from late_decode_search import LateDecodeSimulation
    from linear_reclaim_search import LinearReclaimSimulation
    from memo_search import MemoSimulation
    from no_decode_advice_search import NoDecodeAdviceSimulation
    from numerical_calendar_search import NumericalCalendarSimulation
    from pegaflow.strategies.memory_budget import GIB, Ascend910B2Budget
    from pending_first_search import PendingFirstSimulation
    from prefill_revalidation_search import PrefillRevalidationSimulation
    from prefix_reclaim_search import PrefixReclaimSimulation
    from progress_decode_search import ProgressDecodeSimulation
    from ready_prefill_search import ReadyPrefillSimulation
    from recovery_priority_search import RecoveryPrioritySimulation
    from release_cobatch_search import ReleaseCobatchSimulation
    from reservation_search import ReservationSimulation
    from service_portfolio_search import ServicePortfolioSimulation
    from slot_release_search import SlotReleaseSimulation
    from sparse_cache_search import SparseCacheSimulation
    from timed_recovery_search import (
        TimedDeadlineSimulation,
        TimedMemoSimulation,
        TimedRecoverySimulation,
        TimedReleaseSimulation,
    )
    from yielding_decode_search import YieldingDecodeSimulation

    policies = {
        "memo": MemoSimulation,
        "age": CausalSimulation,
        "laxity": LaxitySimulation,
        "fair-laxity": FairLaxitySimulation,
        "reservation": ReservationSimulation,
        "joint-acquire": JointAcquireSimulation,
        "recovery-priority": RecoveryPrioritySimulation,
        "timed-recovery": TimedRecoverySimulation,
        "recovery-deadline": TimedDeadlineSimulation,
        "release-first": TimedReleaseSimulation,
        "service-portfolio": ServicePortfolioSimulation,
        "bounded-calendar": BoundedSimulation,
        "bounded-proof-memo": ProofMemoSimulation,
        "cache-evolving": CacheEvolvingSimulation,
        "sparse-cache": SparseCacheSimulation,
        "control-release": ControlReleaseSimulation,
        "control-work": ControlWorkSimulation,
        "completed-phase": CompletedPhaseSimulation,
        "acquired-phase": AcquiredPhaseSimulation,
        "pending-first": PendingFirstSimulation,
        "slot-release": SlotReleaseSimulation,
        "release-cobatch": ReleaseCobatchSimulation,
        "delta-protection": DeltaProtectionSimulation,
        "dirty-scan": DirtyScanSimulation,
        "late-decode": LateDecodeSimulation,
        "progress-decode": ProgressDecodeSimulation,
        "yielding-decode": YieldingDecodeSimulation,
        "prefill-revalidation": PrefillRevalidationSimulation,
        "exact-binary-calendar": NumericalCalendarSimulation,
        "linear-reclaim": LinearReclaimSimulation,
        "prefix-reclaim": PrefixReclaimSimulation,
        "no-decode-advice": NoDecodeAdviceSimulation,
        "advice-window": AdviceWindowSimulation,
        "advice-calendar": AdviceCalendarSimulation,
        "ready-prefill": ReadyPrefillSimulation,
    }
    reference = (
        ReleaseCobatchSimulation
        if options["policy"]
        in (
            "delta-protection",
            "dirty-scan",
            "late-decode",
            "progress-decode",
            "yielding-decode",
        )
        else PendingFirstSimulation
        if options["policy"] in ("slot-release", "release-cobatch")
        else AcquiredPhaseSimulation
        if options["policy"] == "pending-first"
        else CompletedPhaseSimulation
        if options["policy"] == "acquired-phase"
        else ControlWorkSimulation
        if options["policy"] == "completed-phase"
        else ControlReleaseSimulation
        if options["policy"] == "control-work"
        else SparseCacheSimulation
        if options["policy"] == "control-release"
        else CacheEvolvingSimulation
        if options["policy"] == "sparse-cache"
        else ProofMemoSimulation
        if options["policy"] == "cache-evolving"
        else TimedMemoSimulation
        if options["policy"]
        in (
            "timed-recovery",
            "recovery-deadline",
            "release-first",
            "service-portfolio",
            "bounded-calendar",
            "bounded-proof-memo",
            "cache-evolving",
            "sparse-cache",
        )
        else sim.BatchSimulation
        if options["policy"] == "memo"
        else FairMemoSimulation
        if options["policy"]
        in ("fair-laxity", "reservation", "joint-acquire", "recovery-priority")
        else MemoSimulation
    )
    if options.get("reference_policy") == "timed-memo":
        reference = TimedMemoSimulation
    if options["policy"] == "prefill-revalidation":
        reference = YieldingDecodeSimulation
    if options["policy"] == "exact-binary-calendar":
        reference = PrefillRevalidationSimulation
    if options["policy"] in ("linear-reclaim", "prefix-reclaim"):
        reference = NumericalCalendarSimulation
    if options["policy"] in ("no-decode-advice", "advice-window", "advice-calendar", "ready-prefill"):
        reference = PrefixReclaimSimulation
    inputs = sim.parent.read(Path(options["inputs"]))
    values = dict(inputs["costs"])
    values["service"] = tuple(
        sim.parent.core.ObservedLatencyModel(**r) for r in values["service"]
    )
    values["decode_seconds"] = tuple(values["decode_seconds"])
    costs = sim.Costs(**values)
    config = sim.BatchConfig(
        max_sequences=4,
        shared_fraction=options.get("decode_sharing_fraction", 0.0),
        bytes_per_token=costs.bytes_per_token,
        kv_bytes=Ascend910B2Budget(
            runtime_reserve_bytes=options["runtime_reserve_gib"] * GIB,
            bytes_per_token=costs.bytes_per_token,
        ).kv_bytes
        if options["kv_gib"] is None
        else options["kv_gib"] * GIB,
    )
    result = {"rate": rate, "config": asdict(config), "arms": {}}
    arms = [
        ("native", "native", policies[options["policy"]]),
        ("reference", "ours", reference),
        ("ours", "ours", policies[options["policy"]]),
    ]
    if options["policy"] == "yielding-decode":
        arms.insert(2, ("parent", "ours", ProgressDecodeSimulation))
    if options["policy"] == "bounded-proof-memo":
        arms.insert(2, ("bounded", "ours", BoundedSimulation))
    if options.get("include_timed_laxity"):
        arms.insert(2, ("laxity", "ours", TimedRecoverySimulation))
    if options.get("paper_baselines"):
        from paper_baseline_search import (
            MemServeSimulation,
            MooncakeSimulation,
            StrataSimulation,
        )

        arms[1:1] = (
            ("mooncake", "ours", MooncakeSimulation),
            ("memserve", "ours", MemServeSimulation),
            ("strata", "native", StrataSimulation),
        )
    if options.get("candidate_first"):
        arms.sort(
            key=lambda arm: ("native", "ours", "parent", "bounded", "reference").index(
                arm[0]
            )
        )
    for name, mode, selected in arms:
        if options.get("staged_save_world"):
            from staged_save_model import StagedSaveSimulationMixin

            selected = type("Staged" + selected.__name__, (StagedSaveSimulationMixin, selected), {})
        experiment = selected(
            inputs["rows"],
            inputs["initial_inventory"],
            costs,
            Path(options["seed"]),
            inputs["control_cost_rows"],
            rate,
            mode,
            options["residual_world"],
            batch_config=config,
            phase_residual_world=options.get("phase_residual_world", False),
        )
        try:
            result["arms"][name] = experiment.run()
        except Exception as error:  # noqa: BLE001 - retain evidence and rethrow a transportable error
            write(Path(options["output"]) / f"case-{index:02d}-{name}-failure.json", {
                "error": repr(error), "at": experiment.now,
                "actions": experiment.actions, "completed_requests": experiment.finished,
                "source": str(repo), "staged_save_world": options.get("staged_save_world", False),
                "partial_not_performance": True,
            })
            # Transport a built-in exception: the parent has not loaded the
            # runtime overlay needed to unpickle custom forecast exceptions.
            raise RuntimeError(repr(error)) from None
        write(
            Path(options["output"]) / f"case-{index:02d}-{name}.json",
            result["arms"][name],
        )
        print(
            json.dumps(
                {
                    "rate_index": index,
                    "arm_completed": name,
                    "p99": result["arms"][name]["TTFT"]["p99"],
                }
            ),
            flush=True,
        )
    result["descriptive_p99_gain"] = (
        1
        - result["arms"]["ours"]["TTFT"]["p99"]
        / result["arms"]["native"]["TTFT"]["p99"]
    )
    if options.get("zero_control_diagnostic"):
        for name in ("ours", "bounded"):
            if name not in result["arms"]:
                continue
            for field in (
                "per_request",
                "stages",
                "resource_events",
                "batch_iterations",
            ):
                if result["arms"]["reference"][field] != result["arms"][name][field]:
                    raise ValueError(f"{name} changed identical-cost {field}")
        result["zero_control_exact_mechanism_equivalence"] = True
    if options.get("fixed_control_diagnostic"):
        result["fixed_control_mechanism_only_not_net_performance"] = True
    write(Path(options["output"]) / f"case-{index:02d}.json", result)
    return index, {name: arm["TTFT"]["p99"] for name, arm in result["arms"].items()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--repo", type=Path, default=Path(__file__).resolve().parents[2]
    )
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--seed", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--policy",
        choices=(
            "memo",
            "age",
            "laxity",
            "fair-laxity",
            "reservation",
            "joint-acquire",
            "recovery-priority",
            "timed-recovery",
            "recovery-deadline",
            "release-first",
            "service-portfolio",
            "bounded-calendar",
            "bounded-proof-memo",
            "cache-evolving",
            "sparse-cache",
            "control-release",
            "control-work",
            "completed-phase",
            "acquired-phase",
            "pending-first",
            "slot-release",
            "release-cobatch",
            "delta-protection",
            "dirty-scan",
            "late-decode",
            "progress-decode",
            "yielding-decode",
            "prefill-revalidation",
            "exact-binary-calendar",
            "linear-reclaim",
            "prefix-reclaim",
            "no-decode-advice",
            "advice-window",
            "advice-calendar",
            "ready-prefill",
        ),
        required=True,
    )
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument(
        "--decode-sharing-fraction",
        type=float,
        default=0.0,
        help="Unmeasured compute-sharing sensitivity, equal in all arms; default zero",
    )
    parser.add_argument(
        "--reference-policy",
        choices=("parent", "timed-memo"),
        default="parent",
        help="Explicit same-world retained-reference check; candidate algorithm stays frozen",
    )
    parser.add_argument("--residual-world", action="store_true")
    parser.add_argument("--phase-residual-world", action="store_true")
    parser.add_argument("--staged-save-world", action="store_true",
                        help="Opt-in source-derived staging/publication lifetime, equal in ALL arms")
    parser.add_argument("--zero-control-diagnostic", action="store_true")
    parser.add_argument("--fixed-control-diagnostic", action="store_true")
    parser.add_argument("--candidate-first", action="store_true")
    parser.add_argument("--include-timed-laxity", action="store_true")
    parser.add_argument(
        "--paper-baselines",
        action="store_true",
        help="Scoped Mooncake/MemServe/Strata adapters, not complete paper systems",
    )
    parser.add_argument(
        "--runtime-reserve-gib",
        type=int,
        default=4,
        help="Unprofiled non-weight runtime headroom for 910B2 CPU scenario",
    )
    parser.add_argument(
        "--kv-gib",
        type=int,
        help="Explicit capacity ablation only; default is 910B2 normal budget",
    )
    args = parser.parse_args()
    if args.staged_save_world and args.policy != "ready-prefill":
        parser.error("Initial staging-world evaluation is scoped to matched ready-prefill / Prefix02")
    if (
        not math.isfinite(args.decode_sharing_fraction)
        or not 0 <= args.decode_sharing_fraction <= 1
    ):
        parser.error("Decode sharing sensitivity must be in [0, 1]")
    if args.reference_policy != "parent" and args.policy not in (
        "pending-first",
        "slot-release",
        "release-cobatch",
    ):
        parser.error(
            "The retained-reference extension belongs to the pending-first candidate"
        )
    if args.phase_residual_world and not args.residual_world:
        parser.error("Joint phase allocation requires --residual-world")
    if args.candidate_first and (
        args.policy
        not in (
            "bounded-proof-memo",
            "cache-evolving",
            "sparse-cache",
            "control-release",
            "control-work",
            "completed-phase",
            "acquired-phase",
            "pending-first",
            "slot-release",
            "release-cobatch",
            "delta-protection",
            "dirty-scan",
            "late-decode",
            "progress-decode",
            "yielding-decode",
        )
        or args.paper_baselines
    ):
        parser.error(
            "Reversed paired order requires an internal candidate without paper arms"
        )
    if args.zero_control_diagnostic and args.policy not in (
        "bounded-calendar",
        "bounded-proof-memo",
        "sparse-cache",
        "delta-protection",
        "dirty-scan",
        "linear-reclaim",
        "prefix-reclaim",
    ):
        parser.error("Zero-control equivalence is only for the exact bounded search")
    if args.fixed_control_diagnostic and args.policy != "exact-binary-calendar":
        parser.error("Fixed-control numerical mechanism diagnostic requires exact-binary-calendar")
    if args.runtime_reserve_gib <= 0 or (args.kv_gib is not None and args.kv_gib <= 0):
        parser.error("Memory sizes must be positive")
    if args.include_timed_laxity and args.policy != "recovery-deadline":
        parser.error("The timed laxity ablation belongs to the deadline screen")
    if not 1 <= args.workers <= 3:
        parser.error("Use one to three CPU workers")
    quota = Path("/sys/fs/cgroup/cpu.max")
    if quota.exists():
        units, period = quota.read_text().split()
        if units != "max" and int(units) / int(period) < args.workers:
            parser.error("Requested workers exceed allocated CPU quota")
    if not args.inputs.is_file() or not args.seed.is_dir():
        parser.error("Explicit verified input file and calibration directory required")
    args.output.mkdir(exist_ok=False)
    options = {
        key: str(value.resolve()) if isinstance(value, Path) else value
        for key, value in vars(args).items()
    }
    root = args.repo / "experiments/issue23_strategies"
    sys.path.insert(0, str(args.repo / "python"))
    from pegaflow.strategies.memory_budget import GIB, Ascend910B2Budget

    memory = Ascend910B2Budget(runtime_reserve_bytes=args.runtime_reserve_gib * GIB)
    write(
        args.output / "memory-budget.json",
        {
            "primary": memory.report(),
            "explicit_kv_override_bytes": args.kv_gib * GIB if args.kv_gib else None,
            "runtime_reserve_sensitivity": [
                Ascend910B2Budget(runtime_reserve_bytes=gib * GIB).report()
                for gib in (2, 4, 8)
            ],
            "host_pool_bytes_per_instance": 2 * GIB,
            "same_memory_for_every_arm": True,
            "provenance": json.loads(
                (root / "memory/910b2-qwen25-bf16.json").read_text(encoding="utf-8")
            ),
        },
    )
    sources = [
        *root.glob("*.py"),
        *root.glob("runtime/overlay/pegaflow/*.py"),
        *args.repo.glob("python/pegaflow/strategies/*.py"),
    ]
    write(
        args.output / "scope.json",
        {
            **options,
            "started": time.time(),
            "input_sha256": hashlib.sha256(args.inputs.read_bytes()).hexdigest(),
            "source_sha256": {
                str(p.relative_to(args.repo)): hashlib.sha256(
                    p.read_bytes()
                ).hexdigest()
                for p in sources
            },
            "CPU_only": True,
            "real_parallel_serving_validated": False,
            "cpu_count": os.cpu_count(),
            "cpu_max": quota.read_text().strip() if quota.exists() else None,
            "shared_compute_fraction": args.decode_sharing_fraction,
            "compute_sharing_is_measured": False,
        },
    )
    rates = (0.24516170769071072, 0.49032341538142143, 0.9806468307628429)
    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        futures = [
            pool.submit(cell, i, rate, options) for i, rate in enumerate(rates, 1)
        ]
        for future in as_completed(futures):
            print(json.dumps({"completed": future.result()}), flush=True)
    cases = [
        json.loads((args.output / f"case-{i:02d}.json").read_text())
        for i in range(1, 4)
    ]
    screen = args.output / "screen.json"
    write(
        screen,
        {
            "cases": cases,
            "input_sha256": hashlib.sha256(args.inputs.read_bytes()).hexdigest(),
        },
    )
    sys.path.insert(0, str(root))
    from analyze_continuous_batch import analyze

    audit = analyze(screen, args.inputs)
    write(args.output / "accounting-audit.json", audit)
    write(
        args.output / "complete.json",
        {"at": time.time(), "validated_arms": audit["validated_arms"]},
    )


if __name__ == "__main__":
    main()
