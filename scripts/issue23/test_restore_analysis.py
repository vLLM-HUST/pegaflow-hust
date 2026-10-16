"""Restore attribution must exclude warmup and retain shared-batch requests."""

import json

import pytest
from analyze_restore import analyze
from rdma_background import command


def test_paced_read_uses_supported_finite_duration():
    argv = command("192.0.2.1", 1234, duration=160, rate_gbps=86, continuous=False)
    assert "--run_infinitely" not in argv
    assert argv[argv.index("-D") + 1] == "160"
    with pytest.raises(ValueError, match="finite-seconds"):
        command("192.0.2.1", 1234, rate_gbps=86)


def test_main_scope_and_shared_load_attribution(tmp_path, capsys):
    services = tmp_path / "services"
    for role in ["D0", "D1"]:
        (services / "telemetry" / role).mkdir(parents=True)
        (services / f"91-{role}-engines.log").write_text("")
    ids = [f"cmpl-tg-test-s17-M-{i:03d}-0-abcd" for i in [0, 1]]
    events = [
        {
            "event": "load_submitted",
            "load_state": "main",
            "request_ids": ids,
            "unix_ns": 1_000_000_000,
        },
        {
            "event": "load_completed",
            "load_state": "main",
            "request_ids": ids,
            "unix_ns": 1_230_000_000,
            "duration_ms": 230,
            "blocks": 2,
        },
        {
            "event": "load_submitted",
            "load_state": "warmup",
            "request_ids": ["cmpl-tg-test-smoke-000-0-abcd"],
            "unix_ns": 500_000_000,
        },
    ]
    (services / "telemetry/D0/events.jsonl").write_text(
        "\n".join(json.dumps(event) for event in events)
    )
    (services / "91-D0-engines.log").write_text(
        "1970-01-01T00:00:01.200000+00:00 INFO Load stage timing: "
        "build_us=1000 api_us=30000 sync_us=40000 copies=2 bytes=1024\n"
        "1970-01-01T00:00:01.200000+00:00 INFO Load task completed: "
        "layers=1 blocks=2 copies=2 bytes=1024 elapsed_ms=71.00 backend=ascend_batch\n"
    )
    output = tmp_path / "restore.json"
    analyze(tmp_path, "test", output)
    report = json.loads(output.read_text())
    assert report["matched_requests"] == 2
    assert report["unmatched_batches"] == 0
    for row in report["requests"]:
        assert row["before_native_ms"] == 129
        assert row["native_api_ms"] == 30
        assert row["native_sync_ms"] == 40
        assert row["completion_observe_ms"] == 30
