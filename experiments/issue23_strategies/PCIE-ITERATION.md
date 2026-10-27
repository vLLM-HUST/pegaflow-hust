# PCIe / endpoint scheduling iteration, 2026-10-26

The research review is [docs/research/tailguard-pcie.md](../../docs/research/tailguard-pcie.md).
The original source05 continuous-batch CPU experiment is retained as the parent;
the archived real worker is still serial. Nothing here enables GPU serving or
proves DMA/compute overlap on Ascend. Timers remain paused.

## What is implemented

`memo_search.py` reuses exactly equal raw candidate forecasts and refreshes
pending work once per immutable decision snapshot. Each new decision gets fresh
costs/progress/layout caches. It keeps the candidate pool and lexicographic tail
objective. It does not prune boundaries or discard physical state.

`causal_prefill.py` implements two alternative orders for **eligible** D prefill
chunks. Decode-first iteration budgeting, memory admission FIFO, device-exclusive
restore/save, full decode, publication and allocation leases stay in place.

For a common prospective conventional-TTFT bound `J`, request `r` has deadline
`ready[r] + J`. The two orders are:

* causal-ready: `ready[r]` (the common J cancels);
* least laxity: `ready[r] - predicted_remaining_prefill[r]` (J and now cancel).

Remaining work uses the issued stage cost and observed completed chunks, not a
future world finish time. Both are local ordering rules, not proofs of global
p99 optimality under memory and I/O contention. The forecast and execution world
use the same rule. A dynamic laxity crossing bounds forecast coalescing, so a
priority change cannot be skipped inside a grouped replay interval.

`fair_endpoint_search.py` repairs a separate simulator discrepancy: the old
execution loop favored node index (P before pending D copies), while forecasts
used request order. A recorded mid-rate control had a 53.88-second pre-copy
wait, with 53.08 seconds covered by later source-P APIs; its link operation took
about 0.0008 seconds. This is software endpoint arbitration in the CPU model,
not observed NIC congestion. The repair keeps every gate and lease, and applies
the same request-order resource arbitration to both arms. Its fresh matrix must
be used; old/new controls cannot be spliced into a claimed paired benefit.

`causal_prefill_search.py` connects these orders to the existing physical
route/source/boundary planner. Native RR remains FIFO. No throughput discount,
bandwidth change, task/rate switch or forced balancing quota is introduced.

## What was rejected

A proposed aggregate-service lower bound failed a full-forecast check in one
high-rate development state (7.1314 versus 6.9804 seconds from ready). Two other
runs not reproducing it do not make that bound safe. Its partial results are
not promoted. Frozen failed sources and diagnostics are retained outside the
maintained policy entry points. The underlying discrepancy still needs a saved
minimal calendar state; this is not a proof of a hardware or cache-state bug.

The causal-ready-only ordering changes real simulated iteration choices, but
its first complete screen had only ~0.8% gains at two rates and a ~4% regression
at the low rate against the same-round memo reference. It is not a selected
upgrade. Laxity is tested next because age alone omits remaining first work.

## Evidence and limits

Every version runs the same 95 inputs / eight full natural sessions, seed and
three nominal rates: 0.2451617077, 0.4903234154, 0.9806468308. Same 32-request
isolated calibration, model/layout/2GiB host + 2GiB KV budget, token128, cap4,
and conservative shared-compute fraction zero. Raw CPU planning wall is charged
to the causal simulation, so a timing change can alter routes, cache evolution
and later inputs' ready times. An equal-objective optimization need not improve
every closed-loop outcome. Native CPU replays are not pure-vLLM hardware runs.

The accounting audit checks all request identities, two stages, causal session
order, model completion cutoffs, physical capacities, token budget, nonoverlap
and TTFT/dependency identities. It does not check model-generated output IDs.
The world and forecast share coarse aggregate costs; H2D task time is not pure
PCIe DMA, and synchronous prefill saves are still embedded in compute body.
The 95 exposed development inputs and max-like p99 are not fresh heldout proof.

`reservation_search.py` adds an optional no-overtaking resource turn for an
already eligible copy. It changes neither current holders nor resource capacity:
older existing work and unrelated endpoints may proceed, younger conflicting
operations wait, and actual device acquisition remains all-or-none. The same
rule is replayed in the forecast. It is an admission/reservation prototype,
not a claim that host-copy service physically needs a GPU gate or that native
server fairness has already been changed. Only admitted eligible copy waiters
are covered; the existing D frontend/capacity FIFO is not silently widened.

## Run a new matrix

Use verified evidence outside the source tree. The public driver refuses an
existing output and never launches a model:

```bash
PYTHONPATH=python:experiments/issue23_strategies \
  /root/issue23-tools/pegaflow-joint-pipeline-01/.venv/bin/python \
  experiments/issue23_strategies/run_pcie_matrix.py \
  --inputs /root/issue23-batch-inputs-20261026/experiments/issue23_handoff/simulation-inputs.json \
  --seed /root/issue23-batch-inputs-20261026/experiments/issue23_strategies/runtime/calibration-seed \
  --output /root/issue23-new-cpu-screen --policy reservation --workers 3
```

`--residual-world` is an explicitly labeled sensitivity using only isolated
calibration residuals; it is not hardware validation. The completed study used
the archived `run_search_matrix.py` driver with frozen per-version source
archives. The new portable CLI was prepared afterward and has not been used to
silently replace those original results.

Tests: `PYTHONPATH=python:experiments/issue23_strategies python -m unittest
test_causal_prefill test_continuous_batch_calendar`. Reference cache reuse was
also compared with fresh full forecasts over 24 requests / 1536 candidates.
The retained source05 kernel/lease checks are in `python/tests/test_continuous_batch.py`.

Exact executed drivers, source delta archives, input hash and complete cases are
stored under the P-node `/root/issue23-pcie-*` evidence directories and copied
locally into the task's `research-20261026/`. Read the completed iteration report
for source hashes and acceptance decisions. Do not substitute a failed/partial
directory for a complete matrix, or use a different version's reference as a
paired control.
