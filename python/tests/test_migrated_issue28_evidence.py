"""Keep original failed model-output gates visible after moving their input data."""
import importlib.util
import os
from pathlib import Path


def verify_packet(name):
    repo = Path(__file__).resolve().parents[2]
    relative = Path("docs/analysis") / name
    spec = importlib.util.spec_from_file_location("issue28_packet", repo / relative / "verify.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    root = Path(os.environ["BENCHMARK_COLLECTION_ROOT"]) / relative
    if name == "issue28-qwen35-hybrid-cache-evidence-20261013":
        return module.verify(root, Path(os.environ["BENCHMARK_ISSUE28_SOURCE_ROOT"]))
    return module.verify(root)


def test_semantics_packet_keeps_failed_output_gate():
    result = verify_packet("issue28-cache-semantics-20261013")
    assert result["strict_output_gate"] == "failed"
    assert result["exact_state_restorations"] == 120
    assert result["valid_loaded_plane_prefixes"] == 3160


def test_hybrid_packet_remains_a_diagnostic():
    result = verify_packet("issue28-qwen35-hybrid-cache-evidence-20261013")
    assert result["model_output_gate"] == "failed"
    assert result["baseline_repeats"] == 18
