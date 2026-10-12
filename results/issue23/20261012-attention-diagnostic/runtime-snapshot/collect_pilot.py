"""Create a reviewable evidence archive after the four-arm pilot finishes."""
import hashlib
import json
import subprocess
import tarfile
import time
from pathlib import Path

ROOT = Path('/data/shared_upload/hust-zsx/tailguard-issue23-20261012')


def main():
    summary = ROOT / 'runs/smoke-pilot-default-summary.json'
    deadline = time.monotonic() + 7200
    while time.monotonic() < deadline:
        if summary.exists():
            value = json.loads(summary.read_text())
            if value['status'] != 'running' and len(value['arms']) == 4:
                break
        time.sleep(5)
    else:
        raise TimeoutError('The pilot did not finish')
    subprocess.run(['python3', str(ROOT / 'runtime/summarize_pilot.py')], check=True)
    paths = [Path(item['run_dir']) for item in value['arms'].values()]
    paths += [ROOT / 'runs/smoke-pilot-default-01-rdma-smooth',
              ROOT / 'runs/qualification-capture-default-05',
              summary, ROOT / 'runs/smoke-pilot-result-evidence.json',
              ROOT / 'prepared/contract.json', ROOT / 'prepared/smoke.json',
              ROOT / 'prepared/plans-smoke', ROOT / 'config/deployment.actual.json',
              ROOT / 'checks/runtime-protocol.json', ROOT / 'checks/reproducibility.json',
              ROOT / 'checks/cpu-tests.xml', ROOT / 'runtime/issue23-pilot.patch']
    setup = ROOT / 'runs/setup-20261012'
    paths += list(setup.glob('runtime-identity-host-*.json'))
    paths += [setup / n for n in ('python-overlay-identity.json', 'connector-tests-final.xml',
                                  'smoke-pilot-orchestrator-06.log', 'qualification-orchestrator-08.log',
                                  'pilot-resume.log')]
    archive = setup / 'pilot-result-evidence.tar.gz'
    with tarfile.open(archive, 'w:gz') as bundle:
        for path in paths:
            bundle.add(path, arcname=str(path.relative_to(ROOT)))
    record = {'status': 'ready', 'archive': str(archive), 'bytes': archive.stat().st_size,
              'sha256': hashlib.sha256(archive.read_bytes()).hexdigest()}
    (setup / 'pilot-collection.json').write_text(json.dumps(record, indent=2) + '\n')
    print('RESULTS_READY', json.dumps(record), flush=True)


if __name__ == '__main__':
    main()
