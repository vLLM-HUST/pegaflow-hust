#!/usr/bin/env python3
import hashlib
import json
import math
import re
from pathlib import Path

FORMAL = Path(__file__).resolve().parent / "formal"
CONCURRENCIES = (1, 2, 4, 8, 16)
NATIVE = {
    1: 91.54555555555555,
    2: 152.7211111111111,
    4: 217.47555555555556,
    8: 291.1066666666667,
    16: 359.75,
}
EXPECTED_WORKLOAD_SHA256 = (
    "8044561ffa1bb430bea8f778ef814d96649321e1a92654b95f64263b996d5e85"
)
EXPECTED_TOKENIZER_FINGERPRINT = (
    "3f9ca78537850303ee04bfa6640c020be89723c62f37121c0f27a4c0babc53e0"
)


def prometheus_values(path: Path) -> dict[str, float]:
    values: dict[str, float] = {}
    for line in path.read_text().splitlines():
        if not line or line.startswith("#"):
            continue
        match = re.match(r"([^\s{]+)(?:\{[^}]*\})?\s+([^\s]+)$", line)
        if not match:
            continue
        name, raw_value = match.groups()
        try:
            value = float(raw_value)
        except ValueError:
            continue
        values[name] = values.get(name, 0.0) + value
    return values


def metric_delta(before: dict[str, float], after: dict[str, float], name: str) -> float:
    return after.get(name, 0.0) - before.get(name, 0.0)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    points = []
    ratios = []
    for concurrency in CONCURRENCIES:
        output_dir = FORMAL / f"c{concurrency}-900s"
        summary = json.loads((output_dir / "summary.json").read_text())
        config = json.loads((output_dir / "config.json").read_text())
        requests = [
            json.loads(line)
            for line in (output_dir / "requests.jsonl").read_text().splitlines()
            if line
        ]

        assert summary["valid"] is True
        assert summary["measurement_seconds"] == 900.0
        assert summary["planned_measurement_seconds"] == 900.0
        assert summary["failed_requests"] == 0
        assert summary["aborted"] is False
        assert len(requests) == summary["requests_started"]
        assert all(request["success"] for request in requests)
        assert all(not request["error"] for request in requests)
        assert all(
            len(request["token_ids"])
            == request["usage"]["completion_tokens"]
            == request["expected_output_tokens"]
            == sum(count for _, count in request["chunks"])
            for request in requests
        )
        assert config["workload_sha256"] == EXPECTED_WORKLOAD_SHA256
        assert config["tokenizer"]["fingerprint"] == EXPECTED_TOKENIZER_FINGERPRINT
        assert config["concurrency"] == concurrency
        assert config["duration"] == 900.0

        vllm_before = prometheus_values(FORMAL / f"metrics-before-c{concurrency}.txt")
        vllm_after = prometheus_values(FORMAL / f"metrics-after-c{concurrency}.txt")
        pega_before = prometheus_values(
            FORMAL / f"pegaflow-metrics-before-c{concurrency}.txt"
        )
        pega_after = prometheus_values(
            FORMAL / f"pegaflow-metrics-after-c{concurrency}.txt"
        )
        vllm_metrics = {
            name: metric_delta(vllm_before, vllm_after, f"vllm:{name}")
            for name in (
                "pega_load_success_total",
                "pega_load_failure_total",
                "pega_save_success_total",
                "pega_save_failure_total",
                "external_prefix_cache_queries_total",
                "external_prefix_cache_hits_total",
                "spec_decode_num_draft_tokens_total",
                "spec_decode_num_accepted_tokens_total",
            )
        }
        pega_metrics = {
            name: metric_delta(pega_before, pega_after, name)
            for name in (
                "pegaflow_cache_block_hits_total",
                "pegaflow_cache_block_misses_total",
                "pegaflow_cache_block_insertions_total",
                "pegaflow_load_bytes_total",
                "pegaflow_save_bytes_total",
            )
        }
        assert vllm_metrics["pega_load_success_total"] > 0
        assert vllm_metrics["pega_save_success_total"] > 0
        assert vllm_metrics["pega_load_failure_total"] == 0
        assert vllm_metrics["pega_save_failure_total"] == 0
        assert vllm_metrics["spec_decode_num_draft_tokens_total"] > 0
        assert vllm_metrics["spec_decode_num_accepted_tokens_total"] > 0
        assert pega_metrics["pegaflow_load_bytes_total"] > 0
        assert pega_metrics["pegaflow_save_bytes_total"] > 0

        candidate = summary["output_tokens_per_second"]
        native = NATIVE[concurrency]
        ratio = candidate / native
        ratios.append(ratio)
        points.append(
            {
                "concurrency": concurrency,
                "candidate_output_tokens_per_second": candidate,
                "native_output_tokens_per_second": native,
                "ratio": ratio,
                "gain_percent": (ratio - 1.0) * 100.0,
                "p90_decode_tokens_per_second_per_user": summary[
                    "decode_tokens_per_second_p90"
                ],
                "output_tokens_in_window": summary["observed_output_tokens_in_window"],
                "requests_started": summary["requests_started"],
                "requests_completed_in_window": summary["requests_completed_in_window"],
                "requests_drained": summary["requests_drained"],
                "failed_requests": summary["failed_requests"],
                "drain_seconds": summary["drain_seconds"],
                "request_token_consistency": True,
                "vllm_mechanism_delta": vllm_metrics,
                "pegaflow_mechanism_delta": pega_metrics,
                "requests_sha256": sha256(output_dir / "requests.jsonl"),
                "summary_sha256": sha256(output_dir / "summary.json"),
            }
        )

    result = {
        "protocol": "swe-prefix-reuse/v1",
        "prepared_workload_sha256": EXPECTED_WORKLOAD_SHA256,
        "tokenizer_fingerprint": EXPECTED_TOKENIZER_FINGERPRINT,
        "native_baseline": "swe-unified-native-20260927",
        "points": points,
        "geomean_ratio": math.prod(ratios) ** (1.0 / len(ratios)),
    }
    result["geomean_gain_percent"] = (result["geomean_ratio"] - 1.0) * 100.0
    (FORMAL / "formal-analysis.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n"
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
