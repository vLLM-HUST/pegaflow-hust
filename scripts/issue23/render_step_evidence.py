"""Recompute development validation, figures and the numerical summary."""

import argparse
import json
from pathlib import Path

from step_simulator import compare, predict, read_json, write_json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    root = parser.parse_args().root
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    model = read_json(root / "cost-model.json")
    results = {}
    plt.rcParams.update({"font.size": 10, "axes.spines.top": False, "axes.spines.right": False})
    fig, axes = plt.subplots(2, 3, figsize=(13.5, 7.2), layout="constrained")
    for row, name in enumerate(("private", "shared")):
        workload = read_json(root / f"data/validation-95-{name}-workload.json.gz")
        observed = read_json(root / f"data/validation-95-{name}-observed.json.gz")
        simulated = predict(workload, model)
        validation = compare(simulated, observed)
        write_json(root / f"{name}-prediction.json.gz", simulated)
        write_json(root / f"{name}-validation.json", validation)
        results[name] = validation
        real = {r["id"]: r for r in observed["requests"]}
        predicted = simulated["requests"]
        ax = axes[row, 0]
        x = [real[r["id"]]["first_s"] for r in predicted]
        y = [r["first_s"] for r in predicted]
        maximum = max(x + y)
        ax.scatter(x, y, s=16, color="#267c8e", alpha=0.75)
        ax.plot([0, maximum], [0, maximum], "--", color="#999", lw=1)
        ax.set(
            title=f"{name.title()} prefixes: first output",
            xlabel="Measured time (s)",
            ylabel="Simulated time (s)",
        )
        for ax, key, title in [
            (axes[row, 1], "waiting", "Waiting requests"),
            (axes[row, 2], "active_KV_blocks", "Active KV pages"),
        ]:
            actual = observed["trajectory"]
            actual_times = [r["at_s"] for r in actual]
            actual_values = [r[key] for r in actual]
            predicted_times = [r["start_s"] for r in simulated["steps"]]
            predicted_values = [r[key] for r in simulated["steps"]]
            if key == "active_KV_blocks":
                actual_times.append(max(r["finish_s"] for r in observed["requests"]))
                actual_values.append(0)
                predicted_times.append(simulated["summary"]["finish_span_s"])
                predicted_values.append(0)
            ax.step(
                actual_times,
                actual_values,
                where="post",
                color="#267c8e",
                lw=1.4,
                label="Measured",
            )
            ax.step(
                predicted_times,
                predicted_values,
                where="post",
                color="#c77b30",
                lw=1.2,
                alpha=0.9,
                label="Simulated",
            )
            ax.set(
                title=f"{name.title()} prefixes: {title}",
                xlabel="Time (s)",
                ylabel=title,
            )
            ax.legend(fontsize=8)
        for ax in axes[row]:
            ax.grid(alpha=0.15)
    fig.suptitle(
        "Single-engine validation: 32 calibration inputs / 95 separate validation inputs\n"
        "Qwen2.5-7B-Instruct BF16 | 910B2 | graph | 2048-token budget | 16 GiB KV",
        fontsize=12,
    )
    fig.savefig(root / "validation.png", dpi=160)
    plt.close(fig)
    summary = {
        "schema": "pegaflow-step-development-validation/v1",
        "scope": "Exposed development workloads, excluded from cost fitting",
        "calibration": {
            "unique_inputs": 32,
            "case_executions": 64,
            "steps": 1326,
            "pure_decode_steps": 1083,
            "mixed_steps": 241,
            "pure_prefill_steps": 2,
        },
        "validation": {
            name: {k: v for k, v in data.items() if k != "per_request_errors"}
            for name, data in results.items()
        },
    }
    write_json(root / "summary.json", summary)
    print(
        json.dumps(
            {name: data["relative_error_percent"] for name, data in results.items()},
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
