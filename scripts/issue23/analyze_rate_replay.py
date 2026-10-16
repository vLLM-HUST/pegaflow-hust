"""Join request latency, normal-path RDMA timings and NPU resource samples."""

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from pathlib import Path

from rate_replay import percentile


def read_jsonl(path):
    if not path.exists():
        return
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            try:
                yield json.loads(line)
            except ValueError:
                continue


def stats(values):
    return {
        "count": len(values),
        "mean": sum(values) / len(values) if values else None,
        "p50": percentile(values, 0.5),
        "p95": percentile(values, 0.95),
        "max": max(values) if values else None,
    }


def npu_samples(root, campaign):
    rows = []
    for node in ["81", "91"]:
        for sample in read_jsonl(root / "services" / f"resources-{node}.jsonl"):
            phase = sample.get("phase")
            try:
                point = (
                    json.loads(phase)["point"]
                    if isinstance(phase, str)
                    else phase["point"]
                )
            except (ValueError, TypeError, KeyError):
                continue
            if not point.startswith("s") or point == "smoke":
                continue
            window = campaign.get(point)
            if window is None:
                continue
            if (
                sample["unix_ns"] < window["first_ns"]
                or sample["unix_ns"] > window["last_ns"]
            ):
                continue
            text = sample.get("npu_smi", {}).get("text", "")
            cores = []
            for line in text.splitlines():
                match = re.search(r"0000:[0-9a-f]+:[0-9a-f]+\.[0-9]\s*\|\s*(\d+)", line)
                if match:
                    cores.append(int(match[1]))
            cpu = {}
            for line in (sample.get("cgroup", {}).get("cpu.stat") or "").splitlines():
                key, value = line.split()
                cpu[key] = int(value)
            rows.append(
                {
                    "node": node,
                    "point": point,
                    "unix_ns": sample["unix_ns"],
                    "aicore": cores,
                    "cpu": cpu,
                    "collector_ms": sample.get("collector_ms"),
                }
            )
    return rows


def analyze(root, campaign_name, output):
    folder = root / "runs" / campaign_name
    output.mkdir(parents=True, exist_ok=True)
    all_requests = [
        r
        for r in read_jsonl(folder / "requests.jsonl")
        if re.fullmatch(r"s\d+-[LMHX]", r["point"])
    ]
    completed_points = {p.parent.name for p in folder.glob("s*/summary.json")}
    requests = [r for r in all_requests if r["point"] in completed_points]
    valid_request_ids = {r["request_id"] for r in requests}
    incomplete = dict(
        Counter(r["point"] for r in all_requests if r["point"] not in completed_points)
    )
    by_point = defaultdict(list)
    for request in requests:
        by_point[request["point"]].append(request)
    timings = defaultdict(dict)
    work = defaultdict(int)
    model_steps = defaultdict(list)
    for role in ["P0", "P1", "D0", "D1"]:
        for path in (root / "services/telemetry" / role).glob("*.jsonl"):
            for event in read_jsonl(path):
                if event.get("event") != "model_work":
                    continue
                active = []
                for request in event.get("requests", []):
                    raw = request.get("request_id", "")
                    if f"tg-{campaign_name}-s" not in raw:
                        continue
                    rid = raw.removeprefix("cmpl-").rsplit("-0-", 1)[0]
                    if rid not in valid_request_ids:
                        continue
                    work[(role, rid)] += request["scheduled_tokens"]
                    active.append(rid)
                if active:
                    model_steps[role].append(len(active))
    for role in ["D0", "D1"]:
        path = root / "services" / f"91-{role}-engines.log"
        if not path.exists():
            continue
        for line in path.read_text().splitlines():
            if f"tg-{campaign_name}-s" not in line or "RDMA fetch " not in line:
                continue
            request = re.search(r"req_id=(\S+)", line)
            if request is None:
                continue
            rid = request[1].removeprefix("cmpl-").rsplit("-0-", 1)[0]
            values = {
                key: float(value)
                for key, value in re.findall(r"(\w+)=([0-9.]+)(?=\s|$)", line)
            }
            timings[rid].update(values)
    report = {
        "campaign": campaign_name,
        "formal_requests_finished": len(requests),
        "incomplete_point_records": incomplete,
        "points": {},
    }
    csv_rows = []
    for point, rows in by_point.items():
        success = [r for r in rows if r["status"] == "ok"]
        summary_path = folder / point / "summary.json"
        summary = (
            json.loads(summary_path.read_text())
            if summary_path.exists()
            else {"status": "running"}
        )
        stages = defaultdict(list)
        for row in success:
            stage = {
                "before_p_s": row["p_submit_s"],
                "p_api_s": row["p_response_s"] - row["p_submit_s"],
                "handoff_s": row["d_submit_s"] - row["p_response_s"],
                "d_first_s": row["d_ttft_s"],
                "after_first_s": row["e2e_s"] - row["ttft_s"],
            }
            for name, value in stage.items():
                stages[name].append(value)
            transport = timings.get(row["request_id"], {})
            csv_rows.append(
                {
                    "request_id": row["request_id"],
                    "point": point,
                    "task": row["task"],
                    "step_id": row["step_id"],
                    "p_id": row["p_id"],
                    "d_id": row["d_id"],
                    "input_tokens": row["input_tokens"],
                    "ttft_s": row["ttft_s"],
                    "tpot_ms": row["tpot_s"] * 1000,
                    "e2e_s": row["e2e_s"],
                    "p_scheduled_tokens": work.get((row["p_id"], row["request_id"])),
                    "d_scheduled_tokens": work.get((row["d_id"], row["request_id"])),
                    "d_prompt_work_tokens": (
                        work[(row["d_id"], row["request_id"])]
                        - row["output_tokens"]
                        + 1
                    )
                    if (row["d_id"], row["request_id"]) in work
                    else None,
                    **stage,
                    "rdma_total_ms": transport.get("total_ms"),
                    "rdma_wait_ms": transport.get("rdma_wait_ms"),
                    "rdma_bytes_mib": transport.get("bytes_mib"),
                }
            )
        report["points"][point] = {
            "summary": summary,
            "stages_s": {k: stats(v) for k, v in stages.items()},
            "rdma_total_ms": stats(
                [
                    timings[r["request_id"]]["total_ms"]
                    for r in success
                    if "total_ms" in timings[r["request_id"]]
                ]
            ),
            "statuses": dict(Counter(r["status"] for r in rows)),
        }
    report["worker_active_sequences"] = {k: stats(v) for k, v in model_steps.items()}
    if requests:
        bounds = {
            point: {
                "first_ns": min(r["started_unix_ns"] for r in point_rows),
                "last_ns": max(
                    r.get("finished_unix_ns", r["started_unix_ns"]) for r in point_rows
                ),
            }
            for point, point_rows in by_point.items()
        }
        samples = npu_samples(root, bounds)
        report["npu"] = {}
        report["cpu"] = {}
        for node in ["81", "91"]:
            for point in by_point:
                values = [
                    value
                    for s in samples
                    if s["node"] == node and s["point"] == point
                    for value in s["aicore"]
                ]
                report["npu"][node + "-" + point] = stats(values)
                observed = sorted(
                    [s for s in samples if s["node"] == node and s["point"] == point],
                    key=lambda s: s["unix_ns"],
                )
                if len(observed) > 1:
                    begin, end = observed[0], observed[-1]
                    elapsed = (end["unix_ns"] - begin["unix_ns"]) / 1e9
                    delta = {
                        k: end["cpu"].get(k, 0) - begin["cpu"].get(k, 0)
                        for k in end["cpu"]
                    }
                    report["cpu"][node + "-" + point] = {
                        "observed_seconds": elapsed,
                        "mean_cpu_cores": delta.get("usage_usec", 0) / 1e6 / elapsed,
                        "throttled_periods": delta.get("nr_throttled", 0),
                        "throttled_usec": delta.get("throttled_usec", 0),
                    }
        (output / "resource-samples.json").write_text(json.dumps(samples))
    if csv_rows:
        with (output / "request-stages.csv").open(
            "w", newline="", encoding="utf-8"
        ) as stream:
            writer = csv.DictWriter(stream, fieldnames=list(csv_rows[0]))
            writer.writeheader()
            writer.writerows(csv_rows)
    (output / "analysis.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))
    return report, csv_rows


def plot(report, rows, output):
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    points = list(report["points"])
    if not points:
        return
    colors = {"L": "#168c84", "M": "#3676bb", "H": "#df803c", "X": "#a5486f"}
    fig, axes = plt.subplots(2, 2, figsize=(12, 8), constrained_layout=True)
    for point in points:
        selected = [r for r in rows if r["point"] == point]
        color = colors[point[-1]]
        axes[0, 0].scatter(
            [r["input_tokens"] / 1000 for r in selected],
            [r["ttft_s"] for r in selected],
            s=14,
            alpha=0.55,
            color=color,
            label=point,
        )
        axes[0, 1].scatter(
            [r["input_tokens"] / 1000 for r in selected],
            [r["tpot_ms"] for r in selected],
            s=14,
            alpha=0.55,
            color=color,
            label=point,
        )
    axes[0, 0].set(xlabel="Input length (1k tokens)", ylabel="End-to-end TTFT (s)")
    axes[0, 1].set(xlabel="Input length (1k tokens)", ylabel="TPOT (ms/token)")
    x, y = [], []
    for point in points:
        summary = report["points"][point]["summary"]
        if summary.get("configured_rate"):
            x.append(summary["configured_rate"])
            y.append(summary["completion_rps"])
    axes[1, 0].scatter(x, y, color="#3676bb", s=40)
    if x:
        axes[1, 0].plot(
            [0, max(x) * 1.1], [0, max(x) * 1.1], ls="--", color="#a6a6a6", lw=1
        )
    axes[1, 0].set(
        xlabel="Configured arrival rate (req/s)",
        ylabel="Completed requests / full run (req/s)",
    )
    bottom = [0.0] * len(points)
    for stage, label, color in [
        ("before_p_s", "Before P", "#a8b6c6"),
        ("p_api_s", "P API span", "#3676bb"),
        ("handoff_s", "KV-ready handoff", "#d9bb75"),
        ("d_first_s", "D submit to first", "#168c84"),
        ("after_first_s", "First token to finish", "#df803c"),
    ]:
        values = [
            report["points"][point]["stages_s"].get(stage, {}).get("mean", 0)
            for point in points
        ]
        axes[1, 1].bar(points, values, bottom=bottom, label=label, color=color)
        bottom = [a + b for a, b in zip(bottom, values, strict=True)]
    axes[1, 1].set(ylabel="Mean request completion latency (s)")
    for axis in axes.flat:
        axis.grid(axis="y", alpha=0.2)
        axis.spines[["top", "right"]].set_visible(False)
    axes[0, 0].legend(fontsize=8)
    axes[1, 1].legend(fontsize=7)
    fig.suptitle(
        "Qwen2.5-7B-Instruct | 2P2D rate replay | 95 fixed inputs per point",
        fontsize=13,
    )
    fig.savefig(output / "rate-replay.png", dpi=180)
    plt.close(fig)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--campaign", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--plot", action="store_true")
    args = parser.parse_args()
    report, rows = analyze(args.root, args.campaign, args.output)
    if args.plot:
        plot(report, rows, args.output)
