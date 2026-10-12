"""Qualify the live P/D deployment and capture the eight-request transfer plan."""
import asyncio
import hashlib
import json
import os
import sys
import time
from pathlib import Path

ROOT = Path('/data/shared_upload/hust-zsx/tailguard-issue23-20261012')
sys.path.insert(0, str(ROOT / 'scripts'))
from client import make_client, stream_request
from common import read_json, read_jsonl, sha256, write_json
from freeze_plan import freeze
from runner import clear_decode, clear_markers, post_control, save_metrics, trace_paths, validate_deployment
import lifecycle

OUTPUT = ROOT / 'runs/qualification-capture-default-05'


def metric_value(path, prefix, status):
    values = []
    for line in path.read_text().splitlines():
        if line.startswith(prefix) and f'status="{status}"' in line:
            values.append(float(line.rsplit(' ', 1)[1]))
    return sum(values)


async def exercise(deployment):
    cases = read_json(ROOT / 'prepared/smoke.json')
    contract = read_json(ROOT / 'prepared/contract.json')
    markers = OUTPUT / 'visibility'
    prepared = OUTPUT / 'prepare'
    prepared.mkdir()
    async with make_client(len(cases)) as client:
        reference_dir = OUTPUT / 'correctness-reference'
        reference_dir.mkdir()
        reference_case = {**cases[0], 'request_id': cases[0]['request_id'] + '-reference',
                          'payload': {**cases[0]['payload'], 'request_id': cases[0]['request_id'] + '-reference', 'max_tokens': 32}}
        reference = await stream_request(client, deployment['prefill_url'], reference_case, reference_dir,
                                         time.monotonic(), 0, contract['timeout_seconds'])
        await post_control(client, deployment['prefill_url'], '/reset_prefix_cache', reference_dir, 'prefill-device-cache')
        for rank in (0, 1):
            await post_control(client, f'http://192.168.0.4:{29091 + rank * 2}', '/cache/memory/cleanup',
                               reference_dir, f'prefill-engine-{rank}-host-cache')
        await save_metrics(client, deployment, OUTPUT, 'before-capture')
        for index, case in enumerate(cases):
            clear_markers(markers, [case], 2)
            await clear_decode(client, deployment, prepared, f'request-{index:04d}')
            request = {**case, 'payload': dict(case['payload'])}
            request['payload']['max_tokens'] = 1 if index else 32
            result = await stream_request(client, deployment['router_url'], request, prepared,
                                          time.monotonic(), 0, contract['timeout_seconds'])
            print('CAPTURED', index, case['request_id'], result['ttft_seconds'], flush=True)
            if not index:
                expected = reference
                match = result['text'] == expected['text'] and result['usage'] == expected['usage']
                write_json(OUTPUT / 'pd-correctness.json', {
                    'exact_text_and_usage_match': match,
                    'baseline_sha256': sha256(reference_dir / 'client.jsonl'),
                    'baseline': expected, 'pd': result})
                if not match:
                    raise ValueError('P/D output differs from the single-node baseline; qualification required')
            visibility = lifecycle.request('prefill', 'check-markers', OUTPUT, request_ids=[case['request_id']])
            if visibility['missing']:
                raise ValueError('Missing physical TP visibility marker')
        await save_metrics(client, deployment, OUTPUT, 'after-capture')
    plans = ROOT / 'prepared/plans-smoke'
    plans.mkdir(exist_ok=True)
    summaries = {}
    for engine, trace in trace_paths(deployment, OUTPUT).items():
        opened = [e for e in read_jsonl(trace) if e.get('event') == 'experiment_opened']
        if len(opened) != 1 or opened[0].get('smooth_scheduler_policy') != 'sched_other' or opened[0].get('smooth_scheduler_priority') != 0:
            raise ValueError(f'Default scheduling identity differs: {engine}')
        summaries[engine] = freeze(ROOT / 'prepared/smoke.json', ROOT / 'prepared/contract.json',
                                   trace, plans / f'{engine}.json', engine)
        plan = read_json(plans / f'{engine}.json')
        expected_bytes = sum(op['logical_payload_bytes'] for request in plan['requests'] for op in request['operations'])
        before = OUTPUT / f'before-capture-{engine}.prom'
        after = OUTPUT / f'after-capture-{engine}.prom'
        fetched = metric_value(after, 'pegaflow_rdma_fetch_bytes', 'ok') - metric_value(before, 'pegaflow_rdma_fetch_bytes', 'ok')
        failed = metric_value(after, 'pegaflow_rdma_fetch_total', 'error') - metric_value(before, 'pegaflow_rdma_fetch_total', 'error')
        if fetched != expected_bytes or failed:
            raise ValueError(f'RDMA completion evidence differs: {engine}, expected={expected_bytes}, fetched={fetched}, errors={failed}')
        summaries[engine]['rdma_completed_bytes'] = int(fetched)
    write_json(plans / 'summary.json', summaries)
    for name in deployment['qualification']:
        deployment['qualification'][name] = name != 'pd_hybrid_correctness'
    deployment['qualification']['pd_output_match'] = True
    deployment['experiment_scope'] = 'attention_transfer_diagnostic'
    deployment['hybrid_prefix_reuse'] = False
    deployment['qualification_evidence'] = str(OUTPUT)
    validate_deployment(deployment)
    write_json(ROOT / 'config/deployment.actual.json', deployment)
    write_json(OUTPUT / 'qualification.json', {'status': 'passed', 'checks': deployment['qualification'],
                                              'transfer_plans': summaries,
                                              'hybrid_prefix_reuse': False,
                                              'note': 'Attention transfer and exact output checked; recurrent membership is local-only, so this pilot includes full D prefill recomputation.'})


def main():
    build = ROOT / 'runs/setup-20261012/build-continuation.log'
    deadline = time.monotonic() + 1800
    while time.monotonic() < deadline:
        content = build.read_text()
        if 'RUNTIME_READY' in content:
            break
        if 'Traceback (most recent call last)' in content:
            raise RuntimeError('Runtime build failed; inspect build-continuation.log')
        time.sleep(5)
    else:
        raise TimeoutError('Runtime build has not completed')
    reuse = os.environ.get('TG_REUSE_ENGINES') == '1'
    OUTPUT.mkdir(exist_ok=reuse)
    os.environ.update(TG_RUN_DIR=str(OUTPUT), TG_MARKER_DIR=str(OUTPUT / 'visibility'), TG_MODE='capture')
    deployment = read_json(ROOT / 'config/deployment.actual.json')
    write_json(OUTPUT / 'run-contract.json', {'mode': 'qualification-capture', 'deployment': deployment,
                                             'contract_sha256': sha256(ROOT / 'prepared/contract.json')})
    status = {'status': 'starting'}
    try:
        if reuse:
            lifecycle.start_models(OUTPUT)
        else:
            lifecycle.start(OUTPUT)
        status['status'] = 'capturing'
        write_json(OUTPUT / 'status.json', status)
        asyncio.run(exercise(deployment))
        status['status'] = 'completed'
    except BaseException as error:
        status.update(status='failed', error=repr(error))
        raise
    finally:
        write_json(OUTPUT / 'status.json', status)
        lifecycle.request('prefill', 'stop', OUTPUT)
        lifecycle.request('decode', 'stop', OUTPUT)


if __name__ == '__main__':
    main()
