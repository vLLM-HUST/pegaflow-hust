"""Run the first frozen four-arm pilot after live qualification finishes."""
import asyncio
import json
import os
import sys
import time
from pathlib import Path

ROOT = Path('/data/shared_upload/hust-zsx/tailguard-issue23-20261012')
sys.path.insert(0, str(ROOT / 'scripts'))
from common import read_json, write_json
from runner import run_one


def main():
    setup = ROOT / 'runs/setup-20261012'
    qualifier = read_json(setup / 'qualification-orchestrator-process-08.json')['pid']
    deadline = time.monotonic() + 1800
    while time.monotonic() < deadline:
        process = Path(f'/proc/{qualifier}/stat')
        if not process.exists() or process.read_text().split(') ', 1)[1].startswith('Z'):
            break
        time.sleep(5)
    else:
        raise TimeoutError('P/D qualification is still pending')
    qualification = read_json(ROOT / 'runs/qualification-capture-default-05/qualification.json')
    if qualification['status'] != 'passed':
        raise ValueError('P/D qualification did not pass')
    deployment = read_json(ROOT / 'config/deployment.actual.json')
    # Record the observed scope explicitly before permitting this diagnostic
    # matrix. Complete hybrid prefix reuse remains a separate qualification.
    deployment['experiment_scope'] = 'attention_transfer_diagnostic'
    deployment['hybrid_prefix_reuse'] = False
    deployment['qualification']['pd_hybrid_correctness'] = False
    deployment['qualification']['pd_output_match'] = True
    write_json(ROOT / 'config/deployment.actual.json', deployment)
    qualification['checks'] = deployment['qualification']
    qualification['experiment_scope'] = deployment['experiment_scope']
    qualification['hybrid_prefix_reuse'] = False
    write_json(ROOT / 'runs/qualification-capture-default-05/qualification.json', qualification)
    order = read_json(ROOT / 'prepared/contract.json')['round_arm_order'][0]
    summary = {'kind': 'four-arm attention-transfer smoke pilot', 'requests_per_arm': 8, 'scheduler_policy': 'sched_other',
               'hybrid_prefix_reuse': False,
               'status': 'running', 'arm_order': order, 'arms': {}}
    path = ROOT / 'runs/smoke-pilot-default-summary.json'
    write_json(path, summary)
    for arm in order:
        label = 'smoke-pilot-default-' + os.environ.get('TG_PILOT_ATTEMPT', '02') + '-' + arm
        print('ARM_START', arm, flush=True)
        try:
            output = asyncio.run(run_one(ROOT, deployment, 'smoke', 'arm', arm, label))
            summary['arms'][arm] = {'status': 'completed', 'run_dir': output,
                                    'analysis': read_json(Path(output) / 'analysis.json')}
            print('ARM_COMPLETED', arm, flush=True)
        except Exception as error:
            output = ROOT / 'runs' / label
            summary['arms'][arm] = {'status': 'failed', 'error': repr(error), 'run_dir': str(output)}
            if (output / 'analysis.json').exists():
                summary['arms'][arm]['analysis'] = read_json(output / 'analysis.json')
            print('ARM_FAILED', arm, repr(error), flush=True)
        write_json(path, summary)
    summary['status'] = 'completed' if all(item['status'] == 'completed' for item in summary['arms'].values()) else 'completed_with_failures'
    write_json(path, summary)
    print('PILOT_FINISHED', summary['status'], flush=True)


if __name__ == '__main__':
    main()
