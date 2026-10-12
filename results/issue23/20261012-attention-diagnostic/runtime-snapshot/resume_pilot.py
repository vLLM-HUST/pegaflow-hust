"""Continue the frozen matrix after saving completed measurements."""
import asyncio
import json
import sys
import time
from pathlib import Path

ROOT = Path('/data/shared_upload/hust-zsx/tailguard-issue23-20261012')
sys.path.insert(0, str(ROOT / 'scripts'))
from common import read_json, write_json
from runner import run_one


def main():
    path = ROOT / 'runs/smoke-pilot-default-summary.json'
    summary = read_json(path)
    second = ROOT / 'runs/smoke-pilot-default-02-local_copy-smooth'
    analysis = read_json(second / 'analysis.json')
    if not analysis['valid_timing_arm'] or analysis['client_requests'] != 8:
        raise ValueError('Completed measurement did not pass independent validation')
    status = read_json(second / 'status.json')
    write_json(second / 'status-before-recovery.json', status)
    status.update(finished_unix=time.time(), recovered_after_measurement=True,
                  teardown_note='Workspace connection interrupted after all responses and metrics; services absent after reconnection.')
    write_json(second / 'status.json', status)
    summary['arms']['local_copy-smooth'] = {'status': 'completed', 'run_dir': str(second),
                                           'analysis': analysis, 'recovered_after_measurement': True}
    write_json(path, summary)
    deployment = read_json(ROOT / 'config/deployment.actual.json')
    for arm in ('rdma-burst', 'local_copy-burst'):
        print('ARM_START', arm, flush=True)
        output = ROOT / 'runs' / ('smoke-pilot-default-03-' + arm)
        try:
            result = asyncio.run(run_one(ROOT, deployment, 'smoke', 'arm', arm, output.name))
            summary['arms'][arm] = {'status': 'completed', 'run_dir': result,
                                    'analysis': read_json(output / 'analysis.json')}
            print('ARM_COMPLETED', arm, flush=True)
        except Exception as error:
            summary['arms'][arm] = {'status': 'failed', 'error': repr(error), 'run_dir': str(output)}
            if (output / 'analysis.json').exists():
                summary['arms'][arm]['analysis'] = read_json(output / 'analysis.json')
            print('ARM_FAILED', arm, repr(error), flush=True)
        write_json(path, summary)
    summary['status'] = 'completed' if all(x['status'] == 'completed' for x in summary['arms'].values()) else 'completed_with_failures'
    write_json(path, summary)
    print('PILOT_FINISHED', summary['status'], flush=True)


if __name__ == '__main__':
    main()
