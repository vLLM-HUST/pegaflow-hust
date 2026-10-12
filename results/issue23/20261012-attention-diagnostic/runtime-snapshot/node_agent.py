"""Execute experiment lifecycle requests from the shared experiment directory."""
import argparse
import hmac
import json
import os
import re
import signal
import subprocess
import time
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path('/data/shared_upload/hust-zsx/tailguard-issue23-20261012')
RUNTIME = ROOT / 'runtime'


def atomic_json(path, value):
    temp = path.with_suffix('.tmp')
    temp.write_text(json.dumps(value, indent=2) + '\n')
    temp.replace(path)


def stop_groups(records):
    for record in reversed(records):
        pid = record['pid']
        try:
            os.killpg(pid, signal.SIGTERM)
        except ProcessLookupError:
            continue
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        groups = {record['pid'] for record in records}
        alive = False
        for stat in Path('/proc').glob('[0-9]*/stat'):
            try:
                fields = stat.read_text().rsplit(') ', 1)[1].split()
                if int(fields[2]) in groups and fields[0] != 'Z':
                    alive = True
                    break
            except (FileNotFoundError, ProcessLookupError):
                pass
        if not alive:
            return
        time.sleep(.5)
    for record in records:
        try:
            os.killpg(record['pid'], signal.SIGKILL)
        except ProcessLookupError:
            pass


def process_job(job, node):
    output = Path(job['run_dir']).resolve()
    if ROOT.resolve() / 'runs' not in output.parents:
        raise ValueError('Run directory outside the experiment')
    output.mkdir(parents=True, exist_ok=True)
    if job['action'] in ('clear-markers', 'check-markers'):
        ids = job.get('request_ids', [])
        if not ids or any(not re.fullmatch(r'[A-Za-z0-9._-]+', rid) for rid in ids):
            raise ValueError('Invalid marker request identity')
        if job['action'] == 'check-markers':
            missing = [f'{rid}.tp{rank}.visible' for rid in ids for rank in (0, 1)
                       if not (output / 'visibility' / f'{rid}.tp{rank}.visible').exists()]
            return {'status': 'checked', 'request_count': len(ids), 'missing': missing}
        for rid in ids:
            for rank in (0, 1):
                (output / 'visibility' / f'{rid}.tp{rank}.visible').unlink(missing_ok=True)
        return {'status': 'cleared', 'request_count': len(ids)}
    metadata = output / f'processes-{node}.json'
    records = json.loads(metadata.read_text()) if metadata.exists() else []
    if job['action'] == 'stop':
        stop_groups(records)
        return {'status': 'stopped', 'processes': records}
    if job['action'] != 'start':
        raise ValueError('Unknown lifecycle action')
    env = os.environ.copy()
    env.pop('ASCEND_RT_VISIBLE_DEVICES', None)
    env.update(job.get('environment', {}))
    allowed = {str(RUNTIME / '.venv/bin/python'),
               *(str(RUNTIME / 'source/target/release' / name)
                 for name in ('pegaflow-server', 'pegaflow-router', 'pegaflow-metaserver'))}
    for specification in job['processes']:
        argv = specification['argv']
        if not argv or argv[0] not in allowed:
            raise ValueError('Executable outside the experiment runtime')
        name = specification['name']
        if not name.replace('-', '').isalnum():
            raise ValueError('Invalid process label')
        with (output / f'{node}-{name}.log').open('ab', buffering=0) as log:
            process = subprocess.Popen(argv, env=env, stdout=log,
                                       stderr=subprocess.STDOUT, start_new_session=True)
        records.append({'name': name, 'pid': process.pid, 'argv': argv})
        atomic_json(metadata, records)
    return {'status': 'started', 'processes': records}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('node', choices=['prefill', 'decode'])
    args = parser.parse_args()
    queue = RUNTIME / 'dispatch' / args.node
    queue.mkdir(parents=True, exist_ok=True, mode=0o700)
    token = (RUNTIME / 'control-token.txt').read_text().strip()
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            if self.path != '/control' or not hmac.compare_digest(self.headers.get('Authorization', ''), 'Bearer ' + token):
                self.send_error(403)
                return
            length = int(self.headers.get('Content-Length', '0'))
            if not 0 < length <= 1000000:
                self.send_error(400)
                return
            job = json.loads(self.rfile.read(length))
            stem = job.pop('control_record')
            if not re.fullmatch('[0-9a-f]{32}', stem):
                self.send_error(400)
                return
            path = queue / f'{stem}.request.json'
            response = queue / f'{stem}.response.json'
            with lock:
                atomic_json(path, job)
                try:
                    result = process_job(job, args.node)
                except Exception as error:
                    result = {'status': 'failed', 'error': repr(error)}
                atomic_json(response, result)
            body = json.dumps(result).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            print(stem, result['status'], flush=True)

        def log_message(self, *_args):
            pass

    ip = '192.168.0.4' if args.node == 'prefill' else '192.168.0.2'
    ThreadingHTTPServer((ip, 29101), Handler).serve_forever()


if __name__ == '__main__':
    main()
