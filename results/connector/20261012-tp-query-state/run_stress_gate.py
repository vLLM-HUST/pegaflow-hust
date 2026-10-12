"""Run the repository warm-hit pressure test with an Ascend launch profile."""
import json
import os
import sys
from pathlib import Path

import pytest
import requests

ROOT = Path(__file__).resolve().parent
SOURCE = ROOT / 'source'
EVIDENCE = ROOT / 'stress-evidence'
EVIDENCE.mkdir(exist_ok=True)
sys.path.insert(0, str(SOURCE / 'python'))
os.environ['VLLM_TEST_ATTN_BACKEND'] = 'CUSTOM'
os.environ['PYTHONPATH'] = str(SOURCE / 'python') + ':' + os.environ.get('PYTHONPATH', '')

from tests import vllm_helpers as helpers
from tests import test_vllm_warm_hit_stress as stress

helpers._detect_pegaflow_cargo_features = lambda: ['ascend', 'rdma']
OriginalPopen = helpers.subprocess.Popen


def launch_npu_process(command, *args, **kwargs):
    if isinstance(command, list) and command[:2] == ['cargo', 'run'] and 'pegaflow-server' in command:
        command = [*command, '--disable-numa-affinity']
    return OriginalPopen(command, *args, **kwargs)


helpers.subprocess.Popen = launch_npu_process
OriginalServer = helpers.PegaFlowServer
OriginalVLLMServer = helpers.VLLMServer


class AscendPegaFlowServer(OriginalServer):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault('pool_size', '1gb')
        super().__init__(*args, **kwargs)

    def __exit__(self, *args):
        try:
            result = helpers.fetch_pegaflow_metrics(self.metrics_port)
            result['rpc_failures'] = helpers.fetch_pegaflow_rpc_failures(self.metrics_port)
            (EVIDENCE / 'metrics.json').write_text(json.dumps(result, indent=2))
        finally:
            super().__exit__(*args)


class AscendVLLMServer(OriginalVLLMServer):
    def __init__(self, *args, **kwargs):
        extra = list(kwargs.pop('extra_args', []))
        extra += ['--enforce-eager', '--kv-cache-memory-bytes', '268435456']
        kwargs['extra_args'] = extra
        kwargs['gpu_memory_utilization'] = 0.2
        kwargs['tensor_parallel_size'] = 2
        kwargs['transfer_backend'] = 'ascend_direct'
        kwargs['startup_timeout'] = 600
        super().__init__(*args, **kwargs)

    def __enter__(self):
        try:
            return super().__enter__()
        except BaseException:
            if self.process and self.process.poll() is not None:
                self.process = None
            self.__exit__(*sys.exc_info())
            raise

    def __exit__(self, *args):
        try:
            if self.process and self.process.poll() is None:
                response = requests.get(f'http://localhost:{self.port}/metrics', timeout=5)
                response.raise_for_status()
                (EVIDENCE / 'vllm-metrics.prom').write_text(response.text)
        except requests.RequestException as error:
            (EVIDENCE / 'metrics-error.txt').write_text(str(error))
        finally:
            super().__exit__(*args)


stress.PegaFlowServer = AscendPegaFlowServer
stress.VLLMServer = AscendVLLMServer
os.chdir(SOURCE / 'python')
code = pytest.main([
    '-m', 'stress', '-s', 'tests/test_vllm_warm_hit_stress.py',
    '--model', '/data/shared_upload/models/Qwen/Qwen3-0.6B',
    '--max-model-len', '2048', '--e2e-port', '28220',
    '--basetemp', str(EVIDENCE / 'pytest'),
    '--junitxml', str(EVIDENCE / 'stress.xml'),
])
if code == 0:
    metrics = json.loads((EVIDENCE / 'metrics.json').read_text())
    assert metrics.get('pegaflow_load_failures_total', 0) == 0
    assert not metrics['rpc_failures']
raise SystemExit(code)
