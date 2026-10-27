# D iteration batching (2026-10-26)

The maintained candidate is based on remote main `9abb470`, obtained via a
verified Git bundle because the P container's GitHub proxy was unavailable.
Original dirty checkouts and byte-frozen executed worker sources are preserved.

## Execution scope

This change implements a **CPU simulation execution model and batch-aware route
calendar**, not an installed parallel vLLM worker. There are currently no usable
NPUs for real validation. The archived worker has `max_num_seqs=1`, a one-request
executor and a full-request engine lock. Those remain historical serial evidence.
Removing that lock or merely increasing Python threads would not safely create
vLLM batching: a future serving adapter needs one engine owner, request-ID based
stream demultiplexing and concurrent-safe, request-scoped transfer accounting.

P0/P1 each execute one prefill at a time. D0/D1 independently admit up to four
sequences. Existing decode sequences get one token in an iteration; remaining
token budget goes to FIFO chunked prefill. The total budget remains 128 tokens,
with 128-token cache blocks. New work can join between iterations. There is no
global two-request limit. Within a session, the next prompt still waits for its
predecessor's full completion.

Each D reserves the full, rounded prompt plus 128 output tokens. Historical
screens used a 2 GiB NPU KV budget; new defaults use the explicit
[910B2 capacity scenario](../../docs/research/910b2-memory-20261026.md), about
36.67 GiB after weights and assumed runtime headroom. Real workers use memory
profiling, not that estimated fixed value. Maximum sequence count is a capacity setting, not a forced
concurrency target or a fitted performance threshold. This conservative model
does not assume prefix sharing inside NPU memory, swapping or preemption.

## Cost and resource assumptions

The isolated 32-request calibration supplies singleton prefirst, restore and
post-first costs. H2D/restore is device-exclusive. The measured post-last-token
API completion component is kept as an exclusive completion barrier. Remaining
prefill cost still includes synchronous per-chunk save and control work; it is
never discounted through batching. Neither timer is claimed to be pure DMA.
Copy retains source/destination device gates, directional software serialization,
finite host allocation, lease protection and publication/fallback behavior.
No RDMA congestion penalty is added.

For one iteration, with prefill singleton work `p_i` and decode work `d_j`:

```
step_seconds = sum(p_i) + (1 - s) * sum(d_j) + s * max(d_j)
```

`s=0` conserves all singleton work. `s=0.5` is an explicitly **unmeasured
sensitivity scenario** in which some decode work is amortized. It is not an
estimated device parameter or a promise of acceleration. With one sequence the
formula reduces exactly to singleton work. Context-sensitive singleton body cost
is distributed across prefill chunks; actual per-chunk kernel scaling remains
unmeasured. The forecast reuses the same batch kernel and physical budgets, and
uses only observed token/chunk progress plus past-completed singleton fits. It
conservatively restarts an acquired iteration at the forecast origin. Fully
repeated iterations may be coalesced only after resolving both devices' next
phase boundaries and all gate waiters, stopping at the next external event or
the first phase completion. Cross-device copy regression tests compare
coalesced and literal steps with unequal device costs. Equivalent forecasts
are memoized within one immutable decision snapshot;
this does not reduce the candidate budget or omit physical constraints.

Batched residence time includes other requests' work. It is not fed back as an
isolated D compute sample. P and completed-copy models retain their existing
past-completed updates. Policy source enumeration, published-prefix feasibility,
64 route / 64 boundary candidate budgets and pending-first tail objective remain
the retained parent's; its serial calendar is replaced. Slow placement stays off.
The baseline remains the same-stack native RR strategy, not untouched vLLM.

Concurrent cache publication exposed a serial-simulator lease bug: releasing by
the current logical hashes could touch a newly published allocation that was
never acquired. The candidate now stores exact allocation identities per
request/operation lease, preserves original protected allocations, and checks
that acquired objects still exist at release. No current-hash lookup is used
to release a handle. Earlier development screens are retained as diagnostics,
not accepted parallel results; the complete matrix must use this fix.

## Reproduce on P CPU

Retrieve external historical inputs according to `EVIDENCE.md`:

```sh
python scripts/fetch_mod_consumer_evidence.py --config BENCHMARK_COLLECTION.json \
  --destination /root/benchmark-collection-NEW
export PYTHONPATH=$PWD/python
python experiments/issue23_strategies/simulate_continuous_batch.py \
  --input-bundle /root/benchmark-collection-NEW/experiments/issue23_handoff/simulation-inputs.json \
  --seed /root/benchmark-collection-NEW/experiments/issue23_strategies/runtime/calibration-seed \
  --output /root/NEW-screen.json
python experiments/issue23_strategies/analyze_continuous_batch.py \
  --input-bundle /root/benchmark-collection-NEW/experiments/issue23_handoff/simulation-inputs.json \
  --screen /root/NEW-screen.json --output /root/NEW-analysis.json
```

The default matrix is 95 unchanged requests/eight sessions, three original
Poisson nominal rates (0.2451617077 / 0.4903234154 / 0.9806468308), seed 20261014,
serial cap=1 and parallel cap=4, with both batch-scaling scenarios for cap=4.
Baseline and treatment have identical execution budgets. Actual causal ingress
remains endogenous. TTFT means causal-ready to first; nominal latency is separate.
Routing CPU, inventory/control RPC, boundary CPU, copy, restore, save and final
drain remain in virtual time. Live wall CPU is charged, so screen values can vary
with CPU environment; this is not a deterministic hardware performance estimate.
`--residual-world` additionally samples only isolated-calibration residuals.
`--limit` is for local contract smoke checks, never a replacement workload.

Do not infer real batched throughput, output correctness, stable p99 or 100P100D
scaling from this screen. A real device batch/context profile is required before
using it as a quantitative deployment prediction. No automation is resumed.
