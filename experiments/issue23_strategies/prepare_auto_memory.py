"""Stage a NEW serial serving snapshot with automatic KV memory profiling.

Does not edit archived runtime bytes, start a server, or claim parallel serving.
The existing ownership/launcher and source audit must target this new snapshot.
"""

import argparse
import ast
import hashlib
import json
from pathlib import Path


def replace_exact(text, before, after, count):
    if text.count(before) != count:
        raise ValueError(f"Unexpected parent shape: {before!r}")
    return text.replace(before, after)


def prepare(source, destination):
    if destination.exists():
        raise FileExistsError("Never overwrite a serving snapshot")
    texts = {p.name: p.read_text(encoding="utf-8") for p in source.glob("*.py")}
    worker = texts["legacy_worker.py"]
    worker = replace_exact(
        worker,
        '"gpu_memory_utilization": 0.35, "kv_cache_memory_bytes": 2 * 1024**3',
        '"gpu_memory_utilization": 0.9, "kv_cache_memory_bytes": None, "memory_mode": "auto_profile"',
        1,
    )
    worker = replace_exact(worker, "free < 0.35 * total", "free < 0.9 * total", 1)
    worker = replace_exact(
        worker,
        "gpu_memory_utilization=0.35, kv_cache_memory_bytes=2 * 1024**3,",
        "gpu_memory_utilization=0.9,",
        1,
    )
    worker = replace_exact(
        worker,
        "self.llm = LLM(**kwargs)",
        """self.llm = LLM(**kwargs)
        cache = self.llm.llm_engine.vllm_config.cache_config
        blocks = getattr(cache, "num_gpu_blocks", None)
        dump(args.output / "auto-memory-profile.json", {
            "mode": "auto_profile", "gpu_memory_utilization": 0.9,
            "requested_kv_bytes": None, "runtime_visible_bytes": total,
            "num_gpu_blocks": blocks, "block_tokens": cache.block_size,
            "capacity_evidence_complete": isinstance(blocks, int) and blocks > 0,
            "missing_profile_requires_engine_log_review": blocks is None})""",
        1,
    )
    texts["legacy_worker.py"] = worker
    # Audits must require actual profiled capacity, not merely the requested
    # utilization. Per-node block counts must be identical across compared arms.
    shared = replace_exact(
        texts["audit_shared.py"],
        "def audit(p,d,workload):",
        "def audit(p,d,workload):\n    memory_profiles = {}",
        1,
    )
    shared = replace_exact(
        shared,
        'config["kv_cache_memory_bytes"]==2*1024**3',
        '"kv_cache_memory_bytes" not in config and config["gpu_memory_utilization"]==0.9',
        1,
    )
    shared = replace_exact(
        shared,
        "            if configs is None:",
        """            memory = read(worker/"auto-memory-profile.json")
            require(memory["capacity_evidence_complete"] is True and
                    type(memory["num_gpu_blocks"]) is int and memory["num_gpu_blocks"]>0,
                    "actual auto-profile blocks required")
            shape = (memory["runtime_visible_bytes"], memory["num_gpu_blocks"], memory["block_tokens"])
            require(memory_profiles.setdefault(node,shape)==shape,"same actual per-node KV capacity across arms")
            if configs is None:""",
        1,
    )
    texts["audit_shared.py"] = shared
    basic = replace_exact(
        texts["audit.py"],
        '    for phase in ("baseline", "treatment"):',
        '    memory_profiles = {}\n    for phase in ("baseline", "treatment"):',
        1,
    )
    basic = replace_exact(
        basic,
        '            assert configuration["kv_cache_memory_bytes"] == 2 * 1024**3',
        """            assert "kv_cache_memory_bytes" not in configuration
            assert configuration["gpu_memory_utilization"] == 0.9
            memory = read(worker / "auto-memory-profile.json")
            assert memory["capacity_evidence_complete"] is True
            assert type(memory["num_gpu_blocks"]) is int and memory["num_gpu_blocks"] > 0
            shape = (memory["runtime_visible_bytes"], memory["num_gpu_blocks"], memory["block_tokens"])
            assert memory_profiles.setdefault(node, shape) == shape""",
        1,
    )
    texts["audit.py"] = basic
    # Both protocol creation and validation change together, in the new copy.
    texts["run.py"] = replace_exact(
        texts["run.py"],
        '"npu_kv_bytes": 2 * 1024**3,',
        '"npu_kv_bytes": None, "gpu_memory_utilization": 0.9, "memory_mode": "auto_profile",',
        2,
    )
    for name, text in texts.items():
        ast.parse(text, filename=name)
    destination.mkdir(parents=True)
    for name, text in texts.items():
        (destination / name).write_text(text, encoding="utf-8", newline="\n")
    protocol = source.parent / "reference-protocol.json"
    if protocol.exists():
        value = json.loads(protocol.read_text(encoding="utf-8"))
        value.update(
            npu_kv_bytes=None, gpu_memory_utilization=0.9, memory_mode="auto_profile"
        )
        (destination / "reference-protocol.auto.json").write_text(
            json.dumps(value, indent=2), encoding="utf-8"
        )
    manifest = {
        "parent_source_sha256": {
            p.name: hashlib.sha256(p.read_bytes()).hexdigest()
            for p in source.glob("*.py")
        },
        "source_sha256": {
            p.name: hashlib.sha256(p.read_bytes()).hexdigest()
            for p in destination.glob("*.py")
        },
        "GPU_started": False,
        "parallel_serving_implemented": False,
        "archived_parent_unmodified": True,
        "protocol_file_to_stage_as_reference": "reference-protocol.auto.json",
        "runtime_allocation": "profile at 0.9 utilization; no manual KV bytes",
        "host_pool_bytes_per_instance": 2147483648,
        "deployment_status": "new source only; launcher and hardware validation still required",
    }
    (destination / "memory-source.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source", type=Path, default=Path(__file__).parent / "runtime/code"
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(prepare(args.source, args.output), indent=2))
