"""Independent three-rate RR/Strata rerun after the scheduler ownership fix."""

import argparse
import hashlib
import json
import time
from dataclasses import asdict
from pathlib import Path

import simulate_continuous_batch as sim
from analyze_continuous_batch import analyze
from paper_baseline_search import StrataSimulation
from run_pcie_matrix import write
from timed_recovery_search import TimedMemoSimulation


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--seed", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(exist_ok=False)
    inputs = json.loads(args.inputs.read_bytes())
    values = dict(inputs["costs"])
    values["service"] = tuple(
        sim.parent.core.ObservedLatencyModel(**r) for r in values["service"]
    )
    values["decode_seconds"] = tuple(values["decode_seconds"])
    costs = sim.Costs(**values)
    config = sim.BatchConfig(bytes_per_token=costs.bytes_per_token)
    started = time.time()
    cases = []
    for index, rate in enumerate(
        (0.24516170769071072, 0.49032341538142143, 0.9806468307628429), 1
    ):
        case = {"rate": rate, "config": asdict(config), "arms": {}}
        for name, cls in (
            ("native", TimedMemoSimulation),
            ("strata", StrataSimulation),
        ):
            arm = cls(
                inputs["rows"],
                inputs["initial_inventory"],
                costs,
                args.seed,
                inputs["control_cost_rows"],
                rate,
                "native",
                False,
                batch_config=config,
            ).run()
            case["arms"][name] = arm
        case["descriptive_p99_gain"] = (
            1
            - case["arms"]["strata"]["TTFT"]["p99"]
            / case["arms"]["native"]["TTFT"]["p99"]
        )
        write(args.output / f"case-{index:02d}.json", case)
        cases.append(case)
        print(
            json.dumps(
                {
                    "rate": rate,
                    "p99": {k: a["TTFT"]["p99"] for k, a in case["arms"].items()},
                }
            ),
            flush=True,
        )
    root = Path(__file__).resolve().parents[2]
    write(
        args.output / "scope.json",
        {
            "started": started,
            "CPU_only": True,
            "reason": "Strata scheduler-owned admission repair; no changes to other policies",
            "source_sha256": {
                str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
                for p in (
                    *root.glob("experiments/issue23_strategies/*.py"),
                    *root.glob("python/pegaflow/strategies/*.py"),
                )
            },
        },
    )
    screen = args.output / "screen.json"
    write(
        screen,
        {
            "cases": cases,
            "input_sha256": hashlib.sha256(args.inputs.read_bytes()).hexdigest(),
        },
    )
    audit = analyze(screen, args.inputs)
    write(args.output / "accounting-audit.json", audit)
    write(
        args.output / "complete.json",
        {
            "started": started,
            "finished": time.time(),
            "validated_arms": audit["validated_arms"],
        },
    )


if __name__ == "__main__":
    main()
