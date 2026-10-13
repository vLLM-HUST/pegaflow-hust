"""Regression checks for request attribution across repeated benchmark runs."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from analyze_cache_paths import analyze, find_client_id


def test_benchmark_preserves_existing_evidence(tmp_path):
    import asyncio

    import pytest
    from cache_path_benchmark import benchmark

    evidence = tmp_path / "evidence"
    evidence.mkdir()
    saved = evidence / "client.jsonl"
    saved.write_text("preserved\n")
    with pytest.raises(FileExistsError):
        asyncio.run(benchmark(evidence, {}, tmp_path, 1, 16))
    assert saved.read_text() == "preserved\n"


def test_request_id_requires_matching_observation_window():
    clients = {"fixture-cold": {"started_monotonic_ns": 2_000_000_000, "e2e_seconds": 1}}
    assert find_client_id("cmpl-fixture-cold-0-abcd", clients, 2_500_000_000) == "fixture-cold"
    assert find_client_id("cmpl-fixture-cold-0-abcd", clients, 500_000_000) is None
    assert find_client_id("fixture-cold", clients, 4_000_000_000) is None
    assert find_client_id("fixture-cold-other", clients, 2_500_000_000) is None


def test_other_run_with_same_id_does_not_add_prefill_or_loads(tmp_path):
    import json

    run = tmp_path / "run"
    arm = run / "round-00-remote_host"
    arm.mkdir(parents=True)
    rid = "fixture-r00-remote_host"
    client = {
        "request_id": rid,
        "started_monotonic_ns": 2_000_000_000,
        "e2e_seconds": 1,
        "ttft_seconds": 0.2,
        "usage": {"completion_tokens": 16},
    }
    (arm / "client.jsonl").write_text(json.dumps(client) + "\n")
    telemetry = tmp_path / "telemetry"
    telemetry.mkdir()
    events = []
    for observed in (500_000_000, 2_100_000_000, 4_000_000_000):
        events.extend(
            [
                {
                    "event": "model_work",
                    "tp_rank": 0,
                    "monotonic_ns": observed,
                    "requests": [
                        {
                            "request_id": rid,
                            "scheduled_tokens": 10,
                            "computed_tokens": 4096,
                            "prompt_tokens": 4106,
                        }
                    ],
                },
                {
                    "event": "cache_query_ready",
                    "request_id": rid,
                    "monotonic_ns": observed,
                    "wait_ms": 5,
                },
                {
                    "event": "load_completed",
                    "tp_rank": 0,
                    "request_ids": [rid],
                    "monotonic_ns": observed,
                    "duration_ms": 8,
                    "success": True,
                },
            ]
        )
    (telemetry / "fixture.jsonl").write_text("".join(json.dumps(e) + "\n" for e in events))
    result = analyze(run, telemetry)
    assert result["requests"][0]["actual_prefill_tokens"] == 10
    assert result["requests"][0]["first_forward_wait_seconds"] == 0.1
    assert result["aggregates"]["remote_host"]["real_reuse_requests"] == 1
    assert not result["semantic_errors"]
