#!/usr/bin/env python3
"""Build the conservative Issue #4 component-cost table from the selector trace."""

from __future__ import annotations

import argparse
import fnmatch
import json
import re
import statistics
from pathlib import Path
from typing import Any

DEFAULT_ARTIFACT = Path("results/trace-audit/20260813-090821/trace_audit.json")
DEFAULT_POLICY = Path("evidence/custody/artifact-policy.json")
EXPECTED_COMMIT = "80521846ae8c4dc893f527a66cade9ff2c6d0647"
EXPECTED_RECORDS = 96

PREFETCH_PATTERN = re.compile(
    r"Prefetch local-hit timing:.*total_keys=(?P<keys>\d+) "
    r"hit=(?P<hit>\d+) missing=(?P<missing>\d+).*"
    r"key_build=(?P<key_build>[\d.]+)(?P<key_build_unit>ns|µs|ms) "
    r"cache_scan=(?P<cache_scan>[\d.]+)(?P<cache_scan_unit>ns|µs|ms) "
    r"task_schedule=(?P<task_schedule>[\d.]+)(?P<task_schedule_unit>ns|µs|ms) "
    r"total=(?P<total>[\d.]+)(?P<total_unit>ns|µs|ms)"
)
REGISTER_PATTERN = re.compile(r"Registered context batch:")


class AnalysisError(RuntimeError):
    """Raised when the historical evidence no longer satisfies its contract."""


def _duration_us(value: str, unit: str) -> float:
    return float(value) * {"ns": 0.001, "µs": 1.0, "ms": 1000.0}[unit]


def _summary(values: list[float]) -> dict[str, float | int]:
    if not values:
        raise AnalysisError("cannot summarize an empty measurement set")
    return {
        "count": len(values),
        "mean": statistics.fmean(values),
        "median": statistics.median(values),
        "min": min(values),
        "max": max(values),
    }


def _classification(policy: dict[str, Any], artifact: Path) -> tuple[str, str]:
    matches: list[tuple[str, str]] = []
    for classification in ("formal", "exploratory"):
        for rule in policy.get(classification, []):
            pattern = rule.get("path") or rule.get("glob")
            if pattern and fnmatch.fnmatch(artifact.as_posix(), pattern):
                matches.append((classification, str(rule.get("reason") or "")))
    if len(matches) != 1:
        raise AnalysisError(
            f"artifact must match exactly one custody rule, matched={matches!r}"
        )
    return matches[0]


def analyze(root: Path, artifact: Path = DEFAULT_ARTIFACT) -> dict[str, Any]:
    artifact_path = root / artifact
    payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    policy = json.loads((root / DEFAULT_POLICY).read_text(encoding="utf-8"))
    classification, classification_reason = _classification(policy, artifact)
    if classification != "exploratory":
        raise AnalysisError(
            "the 20260813 selector trace must not be promoted to formal"
        )

    environment = payload.get("_env", {})
    records = payload.get("records")
    if environment.get("git_commit") != EXPECTED_COMMIT:
        raise AnalysisError("unexpected selector-trace execution commit")
    if not isinstance(records, list) or len(records) != EXPECTED_RECORDS:
        raise AnalysisError("selector trace must contain exactly 96 records")
    if any(not record.get("ok") for record in records):
        raise AnalysisError("selector trace contains an unsuccessful record")

    consumers = [record for record in records if not record.get("producer")]
    if len(consumers) != 90:
        raise AnalysisError("selector trace must contain exactly 90 consumer records")

    queries: dict[str, Any] = {}
    for query_index in (0, 1, 2):
        shared = [
            record
            for record in consumers
            if record["phase"] == "shared" and record["query_idx"] == query_index
        ]
        isolated = [
            record
            for record in consumers
            if record["phase"] == "isolated" and record["query_idx"] == query_index
        ]
        if len(shared) != 15 or len(isolated) != 15:
            raise AnalysisError(f"Q{query_index} does not have 15 records per arm")
        dma_records = [record for record in shared if float(record["dma_ms"]) > 0]
        queue_observations = [
            float(record["queue_time_ms"])
            for record in shared + isolated
            if float(record["queue_time_ms"]) >= 0
        ]
        queries[f"q{query_index}"] = {
            "shared_ttft_ms": _summary(
                [float(record["ttft_s"]) * 1000 for record in shared]
            ),
            "isolated_ttft_ms": _summary(
                [float(record["ttft_s"]) * 1000 for record in isolated]
            ),
            "prefill_saved_ms": (
                statistics.median(float(record["ttft_s"]) for record in isolated)
                - statistics.median(float(record["ttft_s"]) for record in shared)
            )
            * 1000,
            "h2d_ms": (
                _summary([float(record["dma_ms"]) for record in dma_records])
                if dma_records
                else None
            ),
            "h2d_bytes": sorted({int(record["dma_bytes"]) for record in dma_records}),
            "queue_wait_ms": (
                _summary(queue_observations) if queue_observations else None
            ),
        }

    prefetch_rows: dict[int, list[dict[str, float]]] = {1: [], 76: []}
    registration_events = 0
    log_paths = sorted(artifact_path.parent.glob("logs/arm_*_shared/server.log"))
    if len(log_paths) != 3:
        raise AnalysisError("expected exactly three shared-arm server logs")
    for log_path in log_paths:
        for line in log_path.read_text(encoding="utf-8", errors="replace").splitlines():
            registration_events += bool(REGISTER_PATTERN.search(line))
            match = PREFETCH_PATTERN.search(line)
            if match is None:
                continue
            key_count = int(match["keys"])
            if key_count not in prefetch_rows or int(match["hit"]) != key_count:
                continue
            prefetch_rows[key_count].append(
                {
                    field: _duration_us(match[field], match[f"{field}_unit"])
                    for field in ("key_build", "cache_scan", "task_schedule", "total")
                }
            )

    prefetch: dict[str, Any] = {}
    for key_count, rows in prefetch_rows.items():
        if len(rows) != 12:
            raise AnalysisError(
                f"expected 12 complete {key_count}-block prefetch observations"
            )
        prefetch[str(key_count)] = {
            field: _summary([row[field] for row in rows])
            for field in ("key_build", "cache_scan", "task_schedule", "total")
        }
    if registration_events != 15:
        raise AnalysisError("expected 15 shared-arm startup registration events")

    return {
        "schema_version": "pegaflow.issue4-component-cost/v1",
        "source": artifact.as_posix(),
        "classification": classification,
        "classification_reason": classification_reason,
        "claim_boundary": (
            "selector evidence only; not formal performance, paper, end-to-end, "
            "native-vLLM, cross-system, CUDA, RDMA, or SSD evidence"
        ),
        "records": {"total": len(records), "consumers": len(consumers)},
        "queries": queries,
        "prefetch_us": prefetch,
        "registration": {
            "startup_events": registration_events,
            "duration_available": False,
        },
        "unavailable_components": {
            "host_packing": "no request-scoped duration in the retained trace",
            "pinned_host_copy": "not separated from the aggregate load task",
            "d2h": "no store-task duration in the retained trace",
            "registration_duration": "startup event exists, duration does not",
            "queue_wait": "all queue_time_ms values are -1 (unobserved)",
        },
    }


def render_markdown(report: dict[str, Any]) -> str:
    queries = report["queries"]
    q0 = queries["q0"]
    q2 = queries["q2"]
    prefetch_76 = report["prefetch_us"]["76"]
    prefetch_1 = report["prefetch_us"]["1"]
    return f"""# Issue #4 Ascend component cost table

This table is generated from `{report["source"]}` by
`python3 scripts/analyze_issue4_component_cost.py`. The custody classification is
**{report["classification"]}/history-only**. It is selector evidence only, not a
formal paper, end-to-end, native-vLLM, cross-system, CUDA, RDMA, or SSD result.

| Component | Retained observation | Interpretation |
|---|---:|---|
| Host packing | unavailable | No request-scoped duration was recorded; do not fold it into DMA. |
| Pinned-host copy | unavailable | The retained load-task timer does not separate CPU/pinned copy from H2D. |
| H2D, Q0 (76 blocks, 1,434,451,968 bytes) | n={q0["h2d_ms"]["count"]}; mean {q0["h2d_ms"]["mean"]:.3f} ms; median {q0["h2d_ms"]["median"]:.3f} ms; range {q0["h2d_ms"]["min"]:.3f}–{q0["h2d_ms"]["max"]:.3f} ms | Directly measured aggregate `ascend_batch` load task. |
| H2D, Q2 (1 block, 18,874,368 bytes) | n={q2["h2d_ms"]["count"]}; mean {q2["h2d_ms"]["mean"]:.3f} ms; median {q2["h2d_ms"]["median"]:.3f} ms; range {q2["h2d_ms"]["min"]:.3f}–{q2["h2d_ms"]["max"]:.3f} ms | Directly measured aggregate `ascend_batch` load task. |
| D2H | unavailable | No store-task duration was retained. |
| 76-block lookup/scheduling | n={prefetch_76["total"]["count"]}; median total {prefetch_76["total"]["median"]:.3f} µs (key build {prefetch_76["key_build"]["median"]:.3f}, scan {prefetch_76["cache_scan"]["median"]:.3f}, schedule {prefetch_76["task_schedule"]["median"]:.3f}) | Direct server-log timing; not host KV packing. |
| 1-block lookup/scheduling | n={prefetch_1["total"]["count"]}; median total {prefetch_1["total"]["median"]:.3f} µs (key build {prefetch_1["key_build"]["median"]:.3f}, scan {prefetch_1["cache_scan"]["median"]:.3f}, schedule {prefetch_1["task_schedule"]["median"]:.3f}) | Direct server-log timing; not host KV packing. |
| Block registration | {report["registration"]["startup_events"]} startup events; duration unavailable | Registration occurrence is proven, but no cost can be claimed. |
| Queue wait | unavailable | Every retained `queue_time_ms` value is `-1`; no zero-cost inference is allowed. |

## Selector decision

For Q0, median TTFT falls from {q0["isolated_ttft_ms"]["median"]:.1f} ms to
{q0["shared_ttft_ms"]["median"]:.1f} ms, a retained median saving of
{q0["prefill_saved_ms"]:.1f} ms. The measured H2D load costs {q0["h2d_ms"]["mean"]:.1f} ms
on average. That supports the narrow **GO** decision already recorded in #4: cross-instance
KV reuse can clear the single-node Ascend break-even threshold for this long-prompt Q0
selector case.

It does not close the formal evidence gap. Packing, pinned-copy, D2H, registration duration,
and queue wait require new request-scoped instrumentation, while a current-main run must pass
the artifact-custody gate before any formal or paper claim.
"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--format", choices=("json", "markdown"), default="json")
    parser.add_argument("--check", type=Path)
    args = parser.parse_args()
    report = analyze(args.root.resolve())
    rendered = (
        json.dumps(report, ensure_ascii=False, indent=2) + "\n"
        if args.format == "json"
        else render_markdown(report)
    )
    if args.check:
        expected = (args.root / args.check).read_text(encoding="utf-8")
        if expected != rendered:
            raise SystemExit(f"stale generated file: {args.check}")
    else:
        print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
