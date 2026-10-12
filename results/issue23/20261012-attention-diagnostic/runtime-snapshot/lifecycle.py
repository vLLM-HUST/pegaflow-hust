"""Configure and control the two-node Issue23 deployment."""
import json
import os
import socket
import sys
import time
import urllib.request
import uuid
from pathlib import Path

ROOT = Path('/data/shared_upload/hust-zsx/tailguard-issue23-20261012')
RUNTIME = ROOT / 'runtime'
BIN = RUNTIME / 'source/target/release'
PYTHON = str(RUNTIME / '.venv/bin/python')
NODES = {'prefill': '192.168.0.4', 'decode': '192.168.0.2'}


def request(node, action, output, processes=None, environment=None, request_ids=None):
    queue = RUNTIME / 'dispatch' / node
    queue.mkdir(parents=True, exist_ok=True, mode=0o700)
    stem = uuid.uuid4().hex
    body = json.dumps({'action': action, 'run_dir': str(output),
                      'processes': processes or [], 'environment': environment or {},
                      'request_ids': request_ids or [], 'control_record': stem}).encode()
    token = (RUNTIME / 'control-token.txt').read_text().strip()
    message = urllib.request.Request(f'http://{NODES[node]}:29101/control', data=body,
                                     headers={'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'})
    with urllib.request.urlopen(message, timeout=90) as reply:
        result = json.load(reply)
    if result['status'] == 'failed':
        raise RuntimeError(result['error'])
    return result


def wait_health(urls, timeout):
    pending = set(urls)
    deadline = time.monotonic() + timeout
    while pending and time.monotonic() < deadline:
        for url in list(pending):
            try:
                with urllib.request.urlopen(url + '/health', timeout=2) as response:
                    if response.status == 200:
                        pending.remove(url)
                        print('READY', url, flush=True)
            except Exception:
                pass
        if pending:
            time.sleep(3)
    if pending:
        raise TimeoutError(f'Services did not become ready: {sorted(pending)}')


def common_env(node):
    return {'HCCL_IF_IP': NODES[node], 'GLOO_SOCKET_IFNAME': 'bond1',
            'HF_HUB_OFFLINE': '1', 'TOKENIZERS_PARALLELISM': 'false',
            'PYTHONPATH': str(RUNTIME / '.venv/lib/python3.12/site-packages') + ':' + os.environ.get('PYTHONPATH', ''),
            'LD_LIBRARY_PATH': '/usr/local/python3.12.13/lib:' + os.environ.get('LD_LIBRARY_PATH', ''),
            'OMP_NUM_THREADS': '4', 'TOKIO_WORKER_THREADS': '4',
            'VLLM_SERVER_DEV_MODE': '1', 'PEGAFLOW_ISSUE23_DEFAULT_SCHEDULER': '1'}


def wait_listener(host, port, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2):
                print('LISTENING', host, port, flush=True)
                return
        except OSError:
            time.sleep(1)
    raise TimeoutError(f'Router did not start listening: {host}:{port}')


def start(output):
    meta = [{'name': 'metaserver', 'argv': [str(BIN / 'pegaflow-metaserver'),
             '--addr', '192.168.0.2:29056', '--http-addr', '0.0.0.0:29092']}]
    request('decode', 'start', output, meta, common_env('decode'))
    wait_health(['http://192.168.0.2:29092'], 90)
    for node in NODES:
        processes = []
        for rank in (0, 1):
            argv = [str(BIN / 'pegaflow-server'), '--addr', f'{NODES[node]}:{29055 + rank * 2}',
                    '--http-addr', f'0.0.0.0:{29091 + rank * 2}', '--devices', str(rank),
                    '--pool-size', '15gb', '--disable-numa-affinity', '--enable-prometheus',
                    '--nics', 'mlx5_2', '--qps-per-peer', '2',
                    '--metaserver-addr', 'http://192.168.0.2:29056']
            if node == 'decode':
                engine = f'decode-engine-{rank}'
                argv += ['--issue23-trace-path', str(output / f'trace-{engine}.jsonl')]
                if os.environ['TG_MODE'] == 'arm':
                    argv += ['--issue23-plan', str(Path(os.environ['TG_PLANS_DIR']) / f'{engine}.json'),
                             '--issue23-backend', os.environ['TG_BACKEND'],
                             '--issue23-schedule', os.environ['TG_SCHEDULE']]
            processes.append({'name': f'engine-{rank}', 'argv': argv})
        request(node, 'start', output, processes, common_env(node))
    wait_health([f'http://{ip}:{port}' for ip in NODES.values() for port in (29091, 29093)], 150)

    start_models(output)


def start_models(output):
    reference = json.loads((ROOT / 'runs/model-smoke-zsx91-default-02/process.json').read_text())['argv']
    for node in NODES:
        argv = [PYTHON, *reference[1:]]
        argv.remove('--enforce-eager')
        config = {'kv_connector': 'PegaKVConnector', 'kv_connector_module_path': 'pegaflow.connector',
                  'kv_role': 'kv_both', 'kv_connector_extra_config': {
                      'pegaflow.mode': 'save_only' if node == 'prefill' else 'read_write',
                      'pegaflow.host': f'http://{NODES[node]}', 'pegaflow.port': 29055,
                      'pegaflow.tp_shard_endpoints': [f'http://{NODES[node]}:29055', f'http://{NODES[node]}:29057'],
                      'pegaflow.wait_for_full_prefix': node == 'decode'}}
        argv += ['--kv-transfer-config', json.dumps(config)]
        env = common_env(node)
        env['PYTHONHASHSEED'] = '17'
        if node == 'prefill':
            env.update(PEGAFLOW_SYNC_SAVE_ON_FINISH='1', PEGAFLOW_SAVE_ACK_DIR=os.environ['TG_MARKER_DIR'])
        request(node, 'start', output, [{'name': 'vllm', 'argv': argv}], env)
    wait_health([f'http://{ip}:28180' for ip in NODES.values()], 900)
    router = [str(BIN / 'pegaflow-router'), '--host', '0.0.0.0', '--port', '28190',
              '--prefill', 'http://192.168.0.4:28180', '--decode', 'http://192.168.0.2:28180',
              '--prefill-ack-dir', os.environ['TG_MARKER_DIR'], '--prefill-ack-ranks', '2',
              '--prefill-ack-timeout-seconds', '120']
    request('prefill', 'start', output, [{'name': 'router', 'argv': router}], common_env('prefill'))
    wait_listener('192.168.0.4', 28190, 60)


if __name__ == '__main__':
    output = Path(os.environ['TG_RUN_DIR'])
    if sys.argv[1] == 'start':
        start(output)
    elif sys.argv[1] == 'stop':
        errors = []
        for node in NODES:
            try:
                request(node, 'stop', output)
            except Exception as error:
                errors.append(repr(error))
        if errors:
            raise RuntimeError(str(errors))
    elif sys.argv[1] == 'clear-markers':
        cases = json.loads(Path(os.environ['TG_CASES']).read_text())
        print(request('prefill', 'clear-markers', output,
                      request_ids=[case['request_id'] for case in cases]), flush=True)
    else:
        raise ValueError('Unknown lifecycle action')
