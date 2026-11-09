"""Check cache geometry and transfer paths against physical byte/rate contracts."""

import copy

import pytest
from fit_pipeline_profile import fit_restore
from kv_layout import KVLayout
from pd_multinode_simulator import CalibratedPD
from pd_network import NetworkTopology, max_min_rates
from pd_pipeline_simulator import PipelineSimulation
from pd_serving_simulator import BLOCK_BYTES, HostCache
from prepare_pd_workload import prepare
from test_pd_pipeline_simulator import setup


def test_dense_geometry_uses_kv_heads_per_worker_including_k_and_v():
    layout = KVLayout.from_profile(
        {
            "kv_layout": {
                "block_tokens": 128,
                "cache_domain": "qwen25:bf16:tp1",
                "dense_attention": {
                    "layers_per_worker": 28,
                    "kv_heads_per_worker": 4,
                    "head_dim": 128,
                    "dtype_bytes": 2,
                },
            }
        }
    )
    assert layout.bytes_per_block == 7 * 2**20
    cache = HostCache(layout.bytes_per_block * 2, layout.bytes_per_block)
    cache.add(["a", "b", "c"])
    assert list(cache.keys) == ["b", "c"]
    assert cache.peak == 14 * 2**20


def test_restoration_calibration_preserves_dimensionless_cost_when_page_bytes_change():
    rows = []
    for phase in ["a", "b"]:
        for label, blocks, seconds in [
            ("geometry-b128-r0", 128, 0.25),
            ("mixed-b64", 64, 0.125),
            ("mixed-b128", 128, 0.25),
        ]:
            rows.append(
                {
                    "case": phase + "/" + label,
                    "phase": phase,
                    "node": "D0",
                    "blocks": blocks,
                    "seconds": seconds,
                    "idle_s": seconds,
                    "request_count": 1,
                    "busy_s": 0,
                }
            )
    original, _ = fit_restore(rows)
    changed, _ = fit_restore(rows, KVLayout(bytes_per_block=BLOCK_BYTES // 2))
    assert changed["default_copy_GBps"] == pytest.approx(
        original["default_copy_GBps"] / 2
    )
    assert changed["coefficients"] == pytest.approx(original["coefficients"])


def test_geometry_controls_cpu_gpu_capacity_and_network_and_restore_payloads():
    workload, profile = setup()
    workload["requests"] = workload["requests"][:1]
    profile["restore_model"] = {
        "kind": "capacity",
        "coefficients": [0, 1, 0],
        "default_copy_GBps": 1,
        "busy_slowdown": 1,
    }
    profile["network_seconds_per_byte"] = 1e-8
    cfg = {
        "P": 1,
        "D": 1,
        "P_NPU_KV_GiB": 0.125,
        "D_NPU_KV_GiB": 0.125,
        "P_CPU_GiB": 0.125,
        "D_CPU_GiB": 0.125,
        "network_sharing": "processor_sharing",
    }
    old = PipelineSimulation(workload, profile, cfg)
    alternate = copy.deepcopy(profile)
    alternate["kv_layout"] = {
        "bytes_per_block": BLOCK_BYTES // 2,
        "cache_domain": "half-size-kv",
    }
    new = PipelineSimulation(workload, alternate, cfg)
    assert len(new.pools[-1].blocks) == 2 * len(old.pools[-1].blocks)
    a, b = old.run(), new.run()
    assert b["copy_batches"][0]["work_s"] == pytest.approx(
        a["copy_batches"][0]["work_s"] / 2
    )
    assert (
        sum(t["bytes"] for t in b["transfers"])
        == sum(t["bytes"] for t in a["transfers"]) / 2
    )
    assert b["summary"]["peak_CPU_GiB"][-1] == pytest.approx(
        a["summary"]["peak_CPU_GiB"][-1] / 2
    )


def test_different_block_size_keeps_exact_logits_suffix_and_prefix_identity():
    source = [
        {"id": "x", "task": "s", "prompt_token_ids": [3] * 257, "output_tokens": 4}
    ]
    workload = prepare(source, block_tokens=64)
    assert len(workload["requests"][0]["hashes"]) == 4
    _, profile = setup()
    profile["kv_layout"] = {
        "block_tokens": 64,
        "bytes_per_block": BLOCK_BYTES // 2,
        "cache_domain": "qwen25:bf16:64tokens",
    }
    result = PipelineSimulation(workload, profile).run()
    assert result["summary"]["D_uncached_tokens"] == 1
    assert result["per_request"][0]["stages"]["D"]["cached_tokens"] == 256
    assert result["settings"]["kv_layout"]["block_tokens"] == 64
    del workload["block_tokens"]
    with pytest.raises(ValueError, match="block_tokens differ"):
        PipelineSimulation(workload, profile)


def test_other_model_requires_explicit_geometry():
    workload, profile = setup()
    profile["models"]["D"]["metadata"] = {"model": "another-model"}
    with pytest.raises(ValueError, match="Supply kv_layout"):
        PipelineSimulation(workload, profile)
    profile["models"]["D"]["metadata"] = {
        "model": "Qwen2.5-7B-Instruct",
        "dtype": "float32",
    }
    with pytest.raises(ValueError, match="different KV dtype"):
        PipelineSimulation(workload, profile)


@pytest.mark.parametrize(
    "paths,capacities,expected",
    [
        ({0: ("a",), 1: ("b",)}, {"a": 1, "b": 2}, {0: 1, 1: 2}),
        ({0: ("core",), 1: ("core",)}, {"core": 1}, {0: 0.5, 1: 0.5}),
        (
            {0: ("slow", "core"), 1: ("core",)},
            {"slow": 0.25, "core": 1},
            {0: 0.25, 1: 0.75},
        ),
        (
            {0: ("out", "d0"), 1: ("out", "d1"), 2: ("other", "d1")},
            {"out": 1, "d0": 1, "d1": 1, "other": 1},
            {0: 0.5, 1: 0.5, 2: 0.5},
        ),
    ],
)
def test_max_min_allocation_respects_each_shared_link(paths, capacities, expected):
    rates = max_min_rates(paths, capacities)
    assert rates == pytest.approx(expected)
    for link, limit in capacities.items():
        assert (
            sum(rates[i] for i, route in paths.items() if link in route)
            <= limit + 1e-12
        )


def test_explicit_same_host_cpu_path_never_uses_nic_budget():
    workload, profile = setup()
    workload["requests"] = workload["requests"][:1]
    cfg = {
        "P": 1,
        "D": 1,
        "engine_hosts": ["host", "host"],
        "network_sharing": "processor_sharing",
        "network_Gbps": 0.001,
        "network_topology": {
            "links": {"cpu-copy": {"bandwidth_GBps": 2, "latency_us": 1000}},
            "routes": [{"source": "host", "target": "host", "links": ["cpu-copy"]}],
        },
    }
    result = PipelineSimulation(workload, profile, cfg).run()
    transfer = next(t for t in result["transfers"] if t.get("links"))
    assert transfer["transport"] == "cpu_copy"
    assert transfer["start"] - transfer["queued"] == pytest.approx(0.001)
    assert transfer["end"] - transfer["start"] == pytest.approx(transfer["bytes"] / 2e9)
    assert set(result["summary"]["network_link_bytes"]) == {"cpu-copy"}
    with pytest.raises(ValueError, match="requires PipelineSimulation"):
        CalibratedPD(workload, profile, cfg)


def test_two_independent_nics_share_a_fabric_bottleneck():
    workload, profile = setup()
    workload["requests"] = workload["requests"][:2]
    for i, request in enumerate(workload["requests"]):
        request.update(P=i, D=i)
    links = {
        name: {"bandwidth_GBps": 0.01 if name == "core" else 1}
        for name in ["p0", "p1", "core", "d0", "d1"]
    }
    cfg = {
        "P": 2,
        "D": 2,
        "engine_hosts": ["p0", "p1", "d0", "d1"],
        "network_sharing": "processor_sharing",
        "network_topology": {
            "links": links,
            "routes": [
                {
                    "source": f"p{i}",
                    "target": f"d{i}",
                    "links": [f"p{i}", "core", f"d{i}"],
                }
                for i in range(2)
            ],
        },
    }
    result = PipelineSimulation(workload, profile, cfg).run()
    transfers = [t for t in result["transfers"] if t.get("links")]
    assert len(transfers) == 2
    for transfer in transfers:
        assert transfer["end"] - transfer["start"] == pytest.approx(
            2 * transfer["bytes"] / 1e7
        )
    assert result["summary"]["network_link_bytes"]["core"] == sum(
        t["bytes"] for t in transfers
    )


def test_topology_requires_all_used_paths_to_be_explicit():
    topology = NetworkTopology(
        {
            "links": {"local": {"bandwidth_GBps": 1}},
            "routes": [{"source": "a", "target": "a", "links": ["local"]}],
        },
        default_bytes_per_s=1,
    )
    with pytest.raises(ValueError, match="No network route"):
        topology.path("a", "b")


def test_late_arrival_changes_bandwidth_without_duplicate_completions():
    workload, profile = setup()
    workload["requests"] = workload["requests"][:2]
    workload["requests"][1]["arrival_s"] = 0.3
    for request in workload["requests"]:
        request.update(P=0, D=0)
    cfg = {
        "P": 1,
        "D": 1,
        "network_sharing": "processor_sharing",
        "network_topology": {
            "links": {"wire": {"bandwidth_GBps": 0.01}},
            "routes": [{"source": "P-host", "target": "D-host", "links": ["wire"]}],
        },
    }
    result = PipelineSimulation(workload, profile, cfg).run()
    transfers = [t for t in result["transfers"] if t.get("links")]
    assert len(transfers) == len({t["id"] for t in transfers}) == 2
    first, second = sorted(transfers, key=lambda t: t["start"])
    assert first["start"] < second["start"] < first["end"]
    assert max(t["end"] for t in transfers) - first["start"] == pytest.approx(
        sum(t["bytes"] for t in transfers) / 1e7
    )


def test_sixteen_producers_and_consumers_drain_on_shared_fabric():
    workload, profile = setup()
    prototype = workload["requests"][0]
    workload["requests"] = [
        {
            **prototype,
            "id": f"many-{i}",
            "task": f"s{i}",
            "P": i % 16,
            "D": i * 7 % 16,
            "hashes": [f"unique-{i}-0", f"unique-{i}-1"],
        }
        for i in range(64)
    ]
    links = {
        name: {"bandwidth_GBps": 1}
        for name in [
            "core",
            *[f"p{i}" for i in range(16)],
            *[f"d{i}" for i in range(16)],
        ]
    }
    cfg = {
        "P": 16,
        "D": 16,
        "engine_hosts": [f"p{i}" for i in range(16)] + [f"d{i}" for i in range(16)],
        "network_sharing": "processor_sharing",
        "network_topology": {
            "links": links,
            "routes": [
                {
                    "source": f"p{i}",
                    "target": f"d{i * 7 % 16}",
                    "links": [f"p{i}", "core", f"d{i * 7 % 16}"],
                }
                for i in range(16)
            ],
        },
    }
    result = PipelineSimulation(workload, profile, cfg).run()
    assert result["summary"]["requests"] == 64
    assert result["summary"]["D_uncached_tokens"] == 64
    assert result["summary"]["network_link_bytes"]["core"] == 64 * 2 * BLOCK_BYTES
