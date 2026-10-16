# Restore and CPU RDMA competition: 2026-10-16

[Report](../../../docs/experiments/issue23-restore-contention-20261016.md).

Six completed 95-request points: 540 successes and 30 timeouts. The timeouts
belong to the NUMA-bound high-load point and are retained in failures.jsonl.
Latency CSVs contain successful requests; analysis summaries retain all statuses.

- analysis/baseline: restore-baseline, unbound middle rate.
- analysis/local: restore-local, NUMA-bound middle/high rates.
- analysis/network-before, network-competing, network-after: no background,
  paced CPU READ background, then no background at the low rate.
- background-finite: perftest output and measured full-duration average.
- calibration: unpaced link capacity and initial paced calibration.
- config*.json: actual host pools, instance ports and optional NUMA prefix.
- execution-scripts.json: verbatim background coordination scripts for the recorded layout.
- services/final-npu-*.txt: final empty process tables.

The full raw bundle is restore-contention-evidence.tar.gz, retained at
/data/shared_upload/hust-zsx/issue23-restore-20261016 and in the local result
folder. Its file size is recorded in archive-record.json. This Git subset
contains compact measurements; the shared path is not a public download URL.

Reanalyze a raw experiment directory with analyze_rate_replay.py and
analyze_restore.py, using the campaign names above. Inputs and tokenizer
construction follow docs/issue23-2p2d-rate-replay.md at PR #38.

The background mean covers its complete 160-second run. Sampled RNIC counters
cover aggregate endpoint traffic. The two logical producers share one physical
host and RNIC, as do the consumers. Endpoint restore drift prevents a stable
end-to-end network-effect conclusion from this single ABA sequence.
