"""Small CPU wiring contracts, not a mock claim of NPU correctness."""
import importlib.util
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pegaflow

BASE = Path(__file__).resolve().parent
pegaflow.__path__.insert(0,str(BASE/"runtime/overlay/pegaflow"))
pegaflow.__path__.append(str(BASE.parents[1]/"python/pegaflow"))
sys.path.insert(0,str(BASE/"runtime/code"))
from confirmation_control import ARMS, SERVING_PHASE, Sequence
from pegaflow.strategies.live_adapter import native_pair, request_payload

spec = importlib.util.spec_from_file_location("strategy_shared_worker",BASE/"runtime/code/worker.py")
shared_worker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(shared_worker)

class LiveContractTests(unittest.TestCase):
    def test_native_never_supplies_recovery_cap(self):
        self.assertNotIn("max_prefix_blocks",request_payload({"id":"r"},native_cache=True))
        self.assertEqual(native_pair("r","s",256,0.,0.,0),(0,2))
        self.assertEqual(native_pair("r","s",256,0.,0.,1),(1,3))

    def test_shared_worker_switches_only_per_request_sampling_policy(self):
        worker = object.__new__(shared_worker.Worker)
        fake = SimpleNamespace(SamplingParams=lambda **kwargs:kwargs)
        with patch.dict(sys.modules,{"vllm":fake}):
            native = worker.parameters(request_payload({"id":"r"},native_cache=True),128)
            controlled = worker.parameters(request_payload({"id":"r"},prefix=3),128)
            self.assertEqual(native["extra_args"],{})
            self.assertEqual(controlled["extra_args"]["kv_transfer_params"]["pegaflow_joint"]["max_prefix_blocks"],3)
            with self.assertRaises(ValueError):
                worker.parameters({"policy_native_cache":True,"max_prefix_blocks":0},128)

    def test_six_arms_preserve_case_order_and_common_serving_phase(self):
        self.assertEqual(len(ARMS),6)
        sequence = Sequence()
        for arm,_ in ARMS:
            self.assertEqual(sequence.next_arm({"phase":SERVING_PHASE}),arm)
            sequence.started(arm)
            sequence.check_stop({"phase":SERVING_PHASE})
            sequence.stopped()
        sequence.finish({})

if __name__=="__main__":
    unittest.main()
