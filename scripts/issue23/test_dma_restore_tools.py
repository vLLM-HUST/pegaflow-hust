"""CPU contracts for restore experiment launch and replay tools."""

import asyncio
import gzip
import io
import json
from types import SimpleNamespace

import dma_restore_replay as replay
import dma_restore_tools as tools
import httpx
import pytest


def test_output_boundary_and_child_preload_environment(tmp_path):
    with pytest.raises(ValueError, match="outside"):
        tools.external_output(tools.REPOSITORY / "results")
    library = tmp_path / "staging.so"
    library.touch()
    parent = {"LD_PRELOAD": "other.so", "PATH": "original"}
    environment = tools.staging_environment(library, tmp_path / "dma.jsonl", parent)
    assert environment["LD_PRELOAD"] == f"{library.resolve()}:other.so"
    assert environment["PATH"] == "original"
    assert environment["DMA_STAGE_LOG"] == str((tmp_path / "dma.jsonl").resolve())
    assert parent == {"LD_PRELOAD": "other.so", "PATH": "original"}


def test_reset_waits_for_pending_cache_work(monkeypatch):
    requests = []
    metrics = iter([1, 0])

    def request(endpoint, timeout):
        requests.append(endpoint if isinstance(endpoint, str) else endpoint.full_url)
        if isinstance(endpoint, str):
            value = next(metrics)
            return io.BytesIO(
                (
                    f'vllm:pega_pending_save_requests{{model="m"}} {value}\n'
                    'vllm:pega_pending_prefetches{model="m"} 0\n'
                ).encode()
            )
        result = io.BytesIO(b"{}")
        result.status = 200
        return result

    monkeypatch.setattr(tools.urllib.request, "urlopen", request)
    monkeypatch.setattr(tools.time, "sleep", lambda seconds: None)
    config = {
        "hosts": {"P": "localhost"},
        "instances": [{"id": "P0", "role": "P", "model_port": 8000, "http_port": 9000}],
    }
    assert tools.reset_caches(config, 2) == {"P0": 0}
    assert requests == [
        "http://localhost:8000/metrics",
        "http://localhost:8000/metrics",
        "http://localhost:8000/reset_prefix_cache",
        "http://localhost:9000/cache/memory/cleanup",
    ]


def test_replay_obeys_visibility_and_session_dependencies(tmp_path, monkeypatch):
    """Exercise the HTTP protocol only, with synthetic replies and no hardware."""
    cases = [
        {
            "session": "a",
            "turn": turn,
            "prompt_tokens": 2,
            "prompt_token_ids": [1, 2],
            "qwen_output_tokens": 2,
        }
        for turn in [1, 2]
    ]
    with gzip.open(tmp_path / "workload.jsonl.gz", "wt") as stream:
        stream.write("\n".join(json.dumps(row) for row in cases))
    (tmp_path / "runs").mkdir()
    events = []
    instances = {
        f"{role}{index}": {"role": role, "model_port": 8000 + offset + index}
        for role, offset in [("P", 0), ("D", 2)]
        for index in range(2)
    }

    async def protocol(request):
        if request.method == "GET":
            return httpx.Response(200, json={"epoch_ns": 1})
        payload = json.loads(request.content)
        key = payload["request_id"]
        marker = tmp_path / "visibility" / "P0" / f"{key}.tp0.visible"
        if request.url.port < 8002:
            events.append((key, "P"))

            async def publish():
                await asyncio.sleep(0.005)
                marker.parent.mkdir(parents=True, exist_ok=True)
                marker.touch()

            asyncio.create_task(publish())
            return httpx.Response(200, json={"usage": {"completion_tokens": 1}})
        assert marker.exists()
        events.append((key, "D"))
        return httpx.Response(
            200,
            text='data: {"choices":[{"token_ids":[3,4]}],"usage":{"completion_tokens":2}}\n\n'
            "data: [DONE]\n\n",
            headers={"content-type": "text/event-stream"},
        )

    client = httpx.AsyncClient
    monkeypatch.setattr(
        replay.httpx,
        "AsyncClient",
        lambda **kw: client(transport=httpx.MockTransport(protocol), **kw),
    )
    monkeypatch.setattr(replay, "ROOT", tmp_path)
    monkeypatch.setattr(replay, "INST", instances)
    monkeypatch.setattr(
        replay,
        "CFG",
        {
            "hosts": {"P": "localhost", "D": "localhost"},
            "source_budget_bytes": 2**30,
            "kv_bytes_per_token": 196608,
        },
    )
    args = SimpleNamespace(
        workload=None,
        sessions=0,
        smoke=False,
        label="cpu-contract",
        seed=17,
        rate=1e6,
        routing="random",
        d_inflight=16,
        d_pending_budget_gib=1,
        output_cap=2,
    )
    asyncio.run(replay.run(args))
    assert events == [
        ("cpu-contract-0000", "P"),
        ("cpu-contract-0000", "D"),
        ("cpu-contract-0001", "P"),
        ("cpu-contract-0001", "D"),
    ]
    summary = json.loads((tmp_path / "runs/cpu-contract/summary.json").read_text())
    assert summary["counts"] == {"ok": 2}
    phases = [
        "P_queue_s",
        "source_budget_wait_s",
        "P_service_s",
        "publish_wait_s",
        "D_queue_s",
        "D_to_first_s",
    ]
    assert sum(summary[p]["mean"] for p in phases) == pytest.approx(summary["TTFT_s"]["mean"])


def test_replay_rejects_insufficient_budget_before_submitting(tmp_path, monkeypatch):
    with gzip.open(tmp_path / "workload.jsonl.gz", "wt") as stream:
        stream.write(json.dumps({"session": "s", "turn": 1, "prompt_tokens": 1024}))
    monkeypatch.setattr(replay, "ROOT", tmp_path)
    monkeypatch.setattr(replay, "CFG", {"source_budget_bytes": 1, "kv_bytes_per_token": 1})
    args = SimpleNamespace(workload=None, sessions=0, smoke=False, d_pending_budget_gib=0)
    with pytest.raises(ValueError, match="source budget"):
        asyncio.run(replay.run(args))
    assert not (tmp_path / "runs").exists()
