"""Contracts that determine the replayed workload and arrival experiment."""

from prepare_trajectories import SCOPE, visible_messages
from rate_deployment import command
from rate_replay import Replay, schedule


def test_scope_has_95_main_and_32_calibration_agent_steps():
    assert (
        sum(last - first + 1 for split, _, _, first, last in SCOPE if split == "main")
        == 95
    )
    assert (
        sum(
            last - first + 1
            for split, _, _, first, last in SCOPE
            if split == "calibration"
        )
        == 32
    )


def test_history_preserves_previous_tools_and_excludes_current_answer_and_reasoning():
    steps = [
        {"step_id": 1, "source": "user", "message": "task"},
        {
            "step_id": 2,
            "source": "agent",
            "message": "previous answer",
            "reasoning_content": "private reasoning",
            "tool_calls": [
                {
                    "tool_call_id": "c1",
                    "function_name": "shell",
                    "arguments": {"z": 1, "a": 2},
                }
            ],
            "observation": {"results": [{"content": "combined terminal output"}]},
        },
        {"step_id": 3, "source": "agent", "message": "current answer"},
    ]
    result = visible_messages(steps, 3)
    assert [m["role"] for m in result] == ["user", "assistant", "tool"]
    assert result[-1]["content"] == "combined terminal output"
    assert list(result[1]["tool_calls"][0]["function"]["arguments"]) == ["a", "z"]
    assert "private reasoning" not in str(result) and "current answer" not in str(
        result
    )


def test_rate_changes_time_scale_while_routes_and_inputs_stay_paired():
    cases = [{"request_id": str(i)} for i in range(95)]
    slow, fast = schedule(cases, 17, 1), schedule(cases, 17, 2)
    for left, right in zip(slow, fast, strict=True):
        assert (left["case"], left["p"], left["d"]) == (
            right["case"],
            right["p"],
            right["d"],
        )
        assert left["offset"] == 2 * right["offset"]
    assert {(r["p"], r["d"]) for r in slow} == {(0, 0), (0, 1), (1, 0), (1, 1)}


def test_tp1_engine_scope_matches_global_device_and_uses_normal_cache_path():
    config = {
        "root": "/run/replay",
        "site_packages": "/venv/site-packages",
        "bin": "/bin",
        "meta_host": "192.0.2.1",
        "meta_port": 1234,
        "model_revision": "rev",
        "python": "/venv/python",
        "model_path": "/models/qwen",
    }
    instance = {
        "id": "P1",
        "device": 1,
        "host": "192.0.2.1",
        "role": "P",
        "engine_port": 1235,
        "engine_http_port": 1236,
        "model_port": 1237,
    }
    env, engine, model = command(config, instance)
    assert engine[engine.index("--devices") + 1] == "1"
    assert model[model.index("--tensor-parallel-size") + 1] == "1"
    assert "--issue23-trace-path" not in engine
    assert env["ASCEND_RT_VISIBLE_DEVICES"] == "1"
    assert env["PEGAFLOW_SYNC_SAVE"] == "1"


def test_cache_coverage_includes_immediate_ready_logged_matches(tmp_path):
    services = tmp_path / "services"
    services.mkdir()
    (services / "91-D0-models.log").write_text(
        "INFO req=cmpl-test-input-0-abcd1234 cache_lookup: hit_blocks=8 "
        "computed_blocks=0 hit_tokens=1024 num_tokens=1050\n"
    )
    replay = Replay({"root": str(tmp_path), "instances": []}, tmp_path / "run")
    try:
        rows = replay.path_coverage(
            [{"request_id": "test-input", "input_tokens": 1050}], {}
        )
        assert rows[0]["status"] == "full_blocks"
        assert rows[0]["remote_hit_tokens"] == 1024
    finally:
        replay.events.close()
        replay.results.close()
