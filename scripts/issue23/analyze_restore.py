"""Correlate observed restore spans with existing native load stage logs."""

import argparse
import json
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from analyze_rate_replay import read_jsonl, stats


def native_loads(path):
    pending = None
    rows = []
    for line in path.read_text(errors="replace").splitlines():
        if "Load stage timing:" in line:
            pending = {
                key: int(value)
                for key, value in re.findall(r"(\w+)=(\d+)(?=\s|$)", line)
            }
        elif "Load task completed:" in line:
            fields = dict(re.findall(r"(\w+)=([\w.]+)(?=\s|$)", line))
            if pending is None:
                continue
            end_ns = int(datetime.fromisoformat(line.split()[0]).timestamp() * 1e9)
            duration_ms = float(fields["elapsed_ms"])
            rows.append(
                {
                    **pending,
                    "end_ns": end_ns,
                    "start_ns": end_ns - int(duration_ms * 1e6),
                    "elapsed_ms": duration_ms,
                    "blocks": int(fields["blocks"]),
                    "bytes": int(fields["bytes"]),
                }
            )
            pending = None
    return rows


def analyze(root, campaign, output):
    request_pattern = re.compile(rf"tg-{re.escape(campaign)}-s\d+-[LMHX]-\d+(?:-0-|$)")
    rows = []
    missing = []
    for role in ["D0", "D1"]:
        batches = {}
        for path in (root / "services/telemetry" / role).glob("*.jsonl"):
            for event in read_jsonl(path):
                if event.get("event") not in ("load_submitted", "load_completed"):
                    continue
                if not any(
                    request_pattern.search(rid) for rid in event.get("request_ids", [])
                ):
                    continue
                batches.setdefault(event["load_state"], {})[event["event"]] = event
        native = native_loads(root / "services" / f"91-{role}-engines.log")
        used = set()
        for batch in sorted(
            batches.values(),
            key=lambda b: b.get("load_submitted", {}).get("unix_ns", 0),
        ):
            submit, done = batch.get("load_submitted"), batch.get("load_completed")
            if submit is None or done is None:
                missing.append({"role": role, "batch": batch})
                continue
            candidates = [
                (i, row)
                for i, row in enumerate(native)
                if i not in used
                and row["blocks"] == done["blocks"]
                and submit["unix_ns"] - 10_000_000
                <= row["end_ns"]
                <= done["unix_ns"] + 10_000_000
            ]
            if not candidates:
                missing.append({"role": role, "batch": batch})
                continue
            index, task = min(candidates, key=lambda pair: pair[1]["end_ns"])
            used.add(index)
            for rid in done["request_ids"]:
                if not request_pattern.search(rid):
                    continue
                rows.append(
                    {
                        "role": role,
                        "point": re.search(r"(s\d+-[LMHX])-\d+(?:-0-|$)", rid)[1],
                        "request_id": rid.removeprefix("cmpl-").rsplit("-0-", 1)[0],
                        "observed_ms": done["duration_ms"],
                        "before_native_ms": max(
                            0, (task["start_ns"] - submit["unix_ns"]) / 1e6
                        ),
                        "native_build_ms": task.get("build_us", 0) / 1000,
                        "native_api_ms": task.get("api_us", 0) / 1000,
                        "native_sync_ms": task.get("sync_us", 0) / 1000,
                        "native_total_ms": task["elapsed_ms"],
                        "completion_observe_ms": max(
                            0, (done["unix_ns"] - task["end_ns"]) / 1e6
                        ),
                        "bytes": task["bytes"],
                    }
                )
    grouped = defaultdict(list)
    points = defaultdict(lambda: defaultdict(list))
    for row in rows:
        grouped[row["role"]].append(row)
        grouped["all"].append(row)
        points[row["point"]][row["role"]].append(row)
        points[row["point"]]["all"].append(row)

    def summarize(group):
        return {
            key: stats([r[key] for r in group])
            for key in group[0]
            if key.endswith("_ms")
        }

    report = {
        "campaign": campaign,
        "correlation": "FIFO single load worker per role; matching block count and timestamp window",
        "units": "milliseconds; native API/sync spans include contention and are not isolated DMA timings",
        "matched_requests": len(rows),
        "unmatched_batches": len(missing),
        "roles": {role: summarize(group) for role, group in grouped.items()},
        "points": {
            point: {role: summarize(group) for role, group in roles.items()}
            for point, roles in points.items()
        },
        "requests": rows,
        "unmatched": missing,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2))
    print(
        json.dumps(
            {k: v for k, v in report.items() if k not in ["requests", "unmatched"]},
            indent=2,
        )
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--campaign", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    analyze(args.root, args.campaign, args.output)
