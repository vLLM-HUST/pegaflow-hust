"""Collect client, transfer and timing evidence for the completed pilot."""
import hashlib
import json
from pathlib import Path

ROOT = Path('/data/shared_upload/hust-zsx/tailguard-issue23-20261012')


def read(path):
    return json.loads(path.read_text())


def lines(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def metric(path, prefix, status):
    return sum(float(line.rsplit(' ', 1)[1]) for line in path.read_text().splitlines()
               if line.startswith(prefix) and f'status="{status}"' in line)


def main():
    summary = read(ROOT / 'runs/smoke-pilot-default-summary.json')
    if summary['status'] == 'running' or len(summary['arms']) != 4:
        raise ValueError('The four-arm pilot is incomplete')
    evidence = {'contract_sha256': hashlib.sha256((ROOT / 'prepared/contract.json').read_bytes()).hexdigest(),
                'cases_sha256': hashlib.sha256((ROOT / 'prepared/smoke.json').read_bytes()).hexdigest(),
                'requests_per_arm': 8, 'experiment_scope': 'attention_transfer_diagnostic',
                'hybrid_prefix_reuse': False, 'arms': {}, 'output_comparisons': {}}
    outputs = {}
    for name in summary['arm_order']:
        item = summary['arms'][name]
        run = Path(item['run_dir'])
        analysis = item.get('analysis', {})
        row = {'status': item['status'], 'valid_timing_arm': analysis.get('valid_timing_arm', False),
               'validity_issues': analysis.get('validity_issues', []),
               'ttft_seconds': analysis.get('ttft_seconds'), 'e2e_seconds': analysis.get('e2e_seconds'),
               'accuracy': analysis.get('accuracy'), 'transport': {}, 'engine_timing': {}}
        if (run / 'client.jsonl').exists():
            results = lines(run / 'client.jsonl')
            outputs[name] = {r['request_id']: (r.get('text'), r.get('usage')) for r in results}
            row['successful_requests'] = sum(r['status'] == 'ok' for r in results)
        for rank in (0, 1):
            engine = f'decode-engine-{rank}'
            before, prepared, measured = [run / f'{label}-{engine}.prom'
                                          for label in ('before-prepare', 'after-prepare', 'after-measurement')]
            if all(p.exists() for p in (before, prepared, measured)):
                row['transport'][engine] = {
                    'prepare_fetch_bytes': int(metric(prepared, 'pegaflow_rdma_fetch_bytes', 'ok') - metric(before, 'pegaflow_rdma_fetch_bytes', 'ok')),
                    'measurement_fetch_bytes': int(metric(measured, 'pegaflow_rdma_fetch_bytes', 'ok') - metric(prepared, 'pegaflow_rdma_fetch_bytes', 'ok')),
                    'measurement_fetch_errors': int(metric(measured, 'pegaflow_rdma_fetch_total', 'error') - metric(prepared, 'pegaflow_rdma_fetch_total', 'error'))}
            trace = run / f'trace-{engine}.jsonl'
            if trace.exists():
                events = lines(trace)
                opened = [e for e in events if e.get('event') == 'experiment_opened']
                spreads = [e['posting_spread_ns'] / 1e6 for e in events if e.get('event') == 'cohort_posting_spread']
                row['engine_timing'][engine] = {
                    'policy': opened[0].get('smooth_scheduler_policy') if opened else None,
                    'priority': opened[0].get('smooth_scheduler_priority') if opened else None,
                    'burst_spread_ms': spreads,
                    'posting_lateness_ms': analysis.get('engine_reports', {}).get(engine, {}).get('posting_lateness_ms')}
        evidence['arms'][name] = row
    first = summary['arm_order'][0]
    if first in outputs:
        for name, results in outputs.items():
            ids = set(outputs[first]) | set(results)
            evidence['output_comparisons'][name] = {
                'reference_arm': first,
                'requests_compared': len(ids),
                'exact_text_and_usage_matches': sum(rid in outputs[first] and rid in results and outputs[first][rid] == results[rid] for rid in ids)}
    path = ROOT / 'runs/smoke-pilot-result-evidence.json'
    path.write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(evidence, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
