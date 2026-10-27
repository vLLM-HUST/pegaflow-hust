"""Explicit candidate manifest; preserve the prior wrong-policy run as invalid."""
import hashlib
import json
import os
import signal
import subprocess
import tarfile
import time
from pathlib import Path

base = Path('/root')
source = base / 'issue23-ready-prefill-source-01'
scripts = source / 'experiments/issue23_strategies'
python = '/root/issue23-tools/pegaflow-joint-pipeline-01/.venv/bin/python'
inputs = '/root/issue23-batch-inputs-20261026/experiments/issue23_handoff/simulation-inputs.json'
seed = '/root/issue23-batch-inputs-20261026/experiments/issue23_strategies/runtime/calibration-seed'
receipt = base / 'ready-prefill-source-receipt-01.json'
manifest = json.loads(receipt.read_bytes())
assert manifest['sha256'] == 'e409d33886ca206a49eba847719a4cce999a4f576dd81d794ad6fc54474dfc8d'
for name, sha in manifest['files'].items():
    assert hashlib.sha256((source / name).read_bytes()).hexdigest() == sha, name
prior = json.loads((base / 'issue23-advice-calendar-charged-pipeline-02/result.json').read_bytes())
assert prior['failure'] is None and len(prior['archives']) == 2
assert all(step['exit_code'] == 0 and not step['stop'] for step in prior['steps'])
assert not any('/root/issue23-' in s and '/run_pcie_matrix.py' in s
               for s in subprocess.check_output(['ps', '-eo', 'pid,args'], text=True).splitlines())
quota = Path('/sys/fs/cgroup/cpu.max').read_text().split()
assert quota[0] == 'max' or int(quota[0]) / int(quota[1]) >= 3
env = dict(os.environ, OMP_NUM_THREADS='1', OPENBLAS_NUM_THREADS='1', MKL_NUM_THREADS='1',
           PYTHONPATH=f'{source / "python"}:{scripts}')
pipeline = base / 'issue23-ready-prefill-charged-pipeline-01'
pipeline.mkdir()
started, deadline = time.time(), time.monotonic() + 4500
steps, archives, failure = [], [], None
policy, revision = 'ready-prefill', 'ready-prefill-before-restore-01'


def write(path, data):
    with path.open('x') as f:
        json.dump(data, f, indent=2)


def run(command, label, limit=120):
    began = time.time()
    remaining = deadline - time.monotonic()
    assert remaining > 0
    child = subprocess.Popen(command, env=env, start_new_session=True)
    stop = None
    try:
        code = child.wait(timeout=min(remaining, limit))
    except subprocess.TimeoutExpired:
        stop = 'timeout'
        os.killpg(child.pid, signal.SIGTERM)
        try:
            code = child.wait(timeout=15)
        except subprocess.TimeoutExpired:
            os.killpg(child.pid, signal.SIGKILL)
            code = child.wait()
    record = dict(command=command, pid=child.pid, started=began, finished=time.time(),
                  exit_code=code, stop=stop, limit_seconds=limit)
    steps.append(record)
    write(pipeline / f'{label}-process.json', record)
    if code or stop:
        raise RuntimeError(str(record))


write(pipeline / 'start.json', dict(pid=os.getpid(), started=started, limit_seconds=4500,
      source_sha256=manifest['sha256'], runner_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
      policy=policy, expected_candidate_revision=revision, CPU_only=True,
      previous_mixed_candidate_retained=True, expected_reference='prefix-reclaim-calendar-02'))
try:
    run([python, '-c', 'import simulate_continuous_batch; from ready_prefill_search import ReadyPrefillSimulation; '
                      'from prefix_reclaim_search import PrefixReclaimSimulation; '
                      'assert issubclass(ReadyPrefillSimulation, PrefixReclaimSimulation); '
                      'import analyze_ready_prefill, audit_ready_prefill; print("ready-prefill exact class and audit imported")'],
        'entry-contract',30)
    for label, fraction in [('default', 0), ('sharing', 1)]:
        output = base / f'issue23-ready-prefill-charged-{label}-01'
        assert not output.exists()
        command = [python, str(scripts / 'run_pcie_matrix.py'), '--repo', str(source), '--inputs', inputs,
                   '--seed', seed, '--output', str(output), '--policy', policy, '--workers', '3',
                   '--residual-world', '--phase-residual-world', '--decode-sharing-fraction', str(fraction)]
        assert command[command.index('--policy') + 1] == 'ready-prefill'
        write(pipeline / f'{label}-planned-command.json', command)
        run(command, label, 1800)
        assert (output / 'complete.json').exists()
        for i in (1, 2, 3):
            case = json.loads((output / f'case-{i:02d}.json').read_bytes())
            assert case['arms']['ours']['internal_candidate']['name'] == revision
            assert case['arms']['reference']['internal_candidate']['name'] == 'prefix-reclaim-calendar-02'
        for name, extra, target in [
            ('analyze_control_work.py', ['--inputs', inputs, '--source-receipt', str(receipt)], 'source-control-audit.json'),
            ('analyze_pending_first.py', [], 'work-conservation-audit.json'),
            ('audit_release_estimates.py', ['--inputs', inputs, '--seed', seed], 'release-estimate-audit.json'),
            ('analyze_release_feedback.py', [], 'feedback-alignment.json'),
            ('audit_prefill_revalidation.py', [], 'prefill-revalidation-audit.json'),
            ('analyze_prefill_feedback.py', [], 'numerical-feedback.json'),
            ('audit_ready_prefill.py', [], 'ready-prefill-audit.json'),
            ('analyze_ready_prefill.py', [], 'charged-feedback.json'),
        ]:
            run([python, str(scripts / name), '--result', str(output), *extra,
                 '--output', str(output / target)], label + '-' + name)
        archive = base / f'ready-prefill-completed-{label}-01-full.tar.gz'
        with tarfile.open(archive, 'x:gz') as tar:
            tar.add(output, arcname=output.name)
        record = dict(path=str(archive), sha256=hashlib.sha256(archive.read_bytes()).hexdigest(),
                      bytes=archive.stat().st_size)
        archives.append(record)
        write(pipeline / f'{label}-archive.json', record)
except Exception as error:
    failure = repr(error)
finally:
    write(pipeline / 'result.json', dict(started=started, finished=time.time(), steps=steps,
          archives=archives, failure=failure, no_gpu_work=True))
if failure:
    raise SystemExit(1)
