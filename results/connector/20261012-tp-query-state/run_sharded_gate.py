"""Exercise the unchanged correctness plan against two node-local TP engines."""
import json
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent
SOURCE = ROOT / 'source'
EVIDENCE = ROOT / 'sharded-evidence'
EVIDENCE.mkdir(exist_ok=True)
sys.path.insert(0, str(SOURCE / 'python'))
os.environ['VLLM_TEST_ATTN_BACKEND'] = 'CUSTOM'
os.environ['PYTHONPATH'] = str(SOURCE / 'python') + ':' + os.environ.get('PYTHONPATH', '')

from tests import vllm_helpers as helpers
from tests import test_vllm_e2e_correctness as correctness

helpers._detect_pegaflow_cargo_features = lambda: ['ascend', 'rdma']
OriginalPopen = helpers.subprocess.Popen


def launch_npu_process(command, *args, **kwargs):
    if isinstance(command, list) and command[:2] == ['cargo', 'run'] and 'pegaflow-server' in command:
        command = [*command, '--disable-numa-affinity']
    return OriginalPopen(command, *args, **kwargs)


helpers.subprocess.Popen = launch_npu_process
ACTIVE_SERVERS = []
OriginalServer = helpers.PegaFlowServer
OriginalVLLMServer = helpers.VLLMServer


class ShardedServers(OriginalServer):
    def __enter__(self):
        try:
            super().__enter__()
            ACTIVE_SERVERS.append(self)
            self.second = OriginalServer(log_file=EVIDENCE / 'engine-rank1.log',
                                         pool_size=self.pool_size)
            ACTIVE_SERVERS.append(self.second)
            self.second.__enter__()
            return self
        except BaseException:
            if self.process and self.process.poll() is not None:
                self.process = None
            self.__exit__(*sys.exc_info())
            raise

    def __exit__(self, *args):
        for index, server in reversed(list(enumerate(ACTIVE_SERVERS))):
            try:
                if server.process and server.process.poll() is None:
                    result = helpers.fetch_pegaflow_metrics(server.metrics_port)
                    result['rpc_failures'] = helpers.fetch_pegaflow_rpc_failures(server.metrics_port)
                    (EVIDENCE / f'engine-rank{index}-metrics.json').write_text(json.dumps(result, indent=2))
            except Exception as error:
                (EVIDENCE / f'engine-rank{index}-metrics-error.txt').write_text(str(error))
            finally:
                if server is not self:
                    server.__exit__(*args)
        ACTIVE_SERVERS.clear()
        return super().__exit__(*args)


class AscendVLLMServer(OriginalVLLMServer):
    def __init__(self, *args, **kwargs):
        extra = list(kwargs.pop('extra_args', []))
        extra += ['--enforce-eager', '--kv-cache-memory-bytes', '2147483648',
                  '--max-num-seqs', '16']
        kwargs['extra_args'] = extra
        kwargs['gpu_memory_utilization'] = 0.2
        if kwargs.get('use_pegaflow'):
            kwargs['kv_transfer_config'] = {
                'kv_connector': 'PegaKVConnector', 'kv_role': 'kv_both',
                'kv_connector_module_path': 'pegaflow.connector',
                'kv_connector_extra_config': {
                    'pegaflow.transfer_backend': 'ascend_direct',
                    'pegaflow.tp_shard_endpoints': [f'http://127.0.0.1:{s.grpc_port}' for s in ACTIVE_SERVERS],
                },
            }
            kwargs['env_overrides'] = {
                **kwargs.get('env_overrides', {}),
                'PEGAFLOW_SYNC_SAVE_ON_FINISH': '1',
                'PEGAFLOW_SAVE_ACK_DIR': str(EVIDENCE / 'acks'),
            }
        super().__init__(*args, **kwargs)

    def __enter__(self):
        try:
            return super().__enter__()
        except BaseException:
            if self.process and self.process.poll() is not None:
                self.process = None
            self.__exit__(*sys.exc_info())
            raise


correctness.PegaFlowServer = ShardedServers
correctness.VLLMServer = AscendVLLMServer
original_call = correctness.call_openai_api


def record_call(*args, **kwargs):
    result = original_call(*args, **kwargs)
    with (EVIDENCE / 'completions.jsonl').open('a') as stream:
        stream.write(json.dumps({'port': args[0], 'prompt': args[2], 'result': result}) + '\n')
    return result


correctness.call_openai_api = record_call
os.chdir(SOURCE / 'python')
code = pytest.main([
    '-m', 'e2e', '-s', 'tests/test_vllm_e2e_correctness.py',
    '--model', '/data/shared_upload/models/Qwen/Qwen3-0.6B',
    '--tensor-parallel-size', '2', '--max-model-len', '4096',
    '--pegaflow-transfer-backend', 'ascend_direct', '--pegaflow-pool-size', '1gb',
    '--e2e-port', '28220', '--basetemp', str(EVIDENCE / 'pytest'),
    '--junitxml', str(EVIDENCE / 'correctness.xml'),
])
if code == 0:
    for rank in (0, 1):
        metrics = json.loads((EVIDENCE / f'engine-rank{rank}-metrics.json').read_text())
        assert metrics.get('pegaflow_cache_block_hits_total', 0) > 0
        assert metrics.get('pegaflow_load_failures_total', 0) == 0
        assert not metrics['rpc_failures']
    pairs = [{p.name.removesuffix(f'.tp{rank}.visible') for p in (EVIDENCE / 'acks').glob(f'*.tp{rank}.visible')} for rank in (0, 1)]
    assert pairs[0] and pairs[0] == pairs[1], pairs
    print('Verified physical rank ACK pairs:', len(pairs[0]))
raise SystemExit(code)
