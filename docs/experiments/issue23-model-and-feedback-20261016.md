# Issue23: bottleneck-driven model and reproducible strategy scope

## Objective and intermediate times

The research problem is tail latency under finite reusable-KV capacity and
accelerator-side restore/compute/release contention in causal agent replay.
RDMA congestion is a hypothesis to measure, not a premise.

For request r, a_r is nominal offered time, c_r=max(a_r,e_previous_session)
is causal admission, f_r is the observed first token and e_r is full completion.
The conventional serving objective is Q_0.99({f_r-c_r}); f_r-a_r and c_r-a_r
are reported separately. P end, D first and D full are intermediate predictions,
not three independent optimization outputs. With 95 requests the nearest-rank
p99 is the maximum, not a stable population-tail estimate.

Each operation v has predecessor set pred(v), required resources R_v and
completed-only predicted duration d_v. Its feasible start must satisfy
s_v >= max(c_r, max(e_u for u in pred(v))) and all required resources must be
simultaneously free. e_v=s_v+d_v. FIFO ordering is retained per actual executor;
future D queue order follows P completion, not initial request order.

Service duration decomposition is restore/H2D + compute-to-first + decode tail
+ synchronous save + release. In the current shared API these overlap/nest in
timers: H2D-task time is not added again to the enclosing restore/API duration.
Copy RPC duration and source/destination gate waits are distinct. No fitted
network congestion multiplier or forced 50/50 routing quota is introduced.

A job with an observed first token remains an unfinished resource holder but
does not remain in the mutable pending-first objective. The online objective
is the descending latency profile of admitted pending-first jobs, then the
full-release profile. This is an explicit finite online surrogate, not proof
that full-run p99 is optimal. Layout actions require strict predicted improvement
after their preparation/copy costs and fresh native commit checks. The selector
does not implement commit; native generation/lease/staging receipts still do.

## Baselines and fidelity

All policy modules live in python/pegaflow/strategies. The first real comparison
must use the default vLLM/PegaFlow cache lookup (no our selected restore cap,
routing forecast or layout optimization) behind a declared round-robin proxy.
It is not pure vLLM without PegaFlow, and the proxy is not a vLLM feature.
The old parity baseline selected a custom restore boundary, so it cannot certify
this new naive comparison. Same GPU-prefix authority, budgets, warmup state,
shared worker code and implicit-RDMA provenance must be verified in the adapter.
The existing old worker does not support this contract and must not be reused
unchanged while labeling it native. No new formal comparison has been launched.

MemServe Eq1/Eq2 is implemented as an explicitly named strategy adapter:
queue+local-prefix execution selects P, and remote reuse is considered when
copy cost does not exceed saved execution. Measured copy/compute and least-work
D selection are declared adaptations, not reproduction of MemPool/full MemServe.
Source: https://arxiv.org/html/2406.17565 (Section 5.3).

LMetric's multiplication kernel is implemented but NOT registered as a complete
baseline. Its paper scopes PD colocation, not this disaggregation setting;
the engine batch indicator must not be silently replaced by FIFO queue depth.
Source: https://arxiv.org/html/2603.15202v1.

Mooncake is a relevant system-level comparison, but Transfer Engine alone is
not its Conductor/Store policy. No unsupported full-Mooncake reproduction or
SLO/rejection experiment is claimed here. Compare named strategy adaptations
separately from full-system runs after the naive baseline is beaten.
Source: https://www.usenix.org/system/files/fast25-qin.pdf.

A separately named FAST25 Algorithm1 prefill adapter is now implemented. It
requires an explicitly supplied paper cache-balancing ratio; no benchmark
default is invented or fitted. It retains the strict ratio comparison from
the FAST25 paper, handles zero local prefix by multiplication, and disables
rejection in this all-request scope. Its least-request D selector is declared
as an adapter. This is NOT Conductor/Store/TE or a full-system reproduction.

## Preserved parent and real feedback

The four-arm phase-release source and job under
/root/issue23-phase-release/20261016-source01 remain immutable. The new resource
scheduler is an exact copied file from that parent's operation_schedule.py;
its SHA256 must match. Stable venv/native/Ascend dependencies are not updated.

P-side provisional results (D-side independent archive/audit still pending):

| Arm | Conventional TTFT mean | P50 | P95 | P99/max | Phase seconds |
| --- | ---: | ---: | ---: | ---: | ---: |
| B-before | 34.8341 | 31.3747 | 60.7150 | 74.6612 | 707.2126 |
| T-parent | 33.5772 | 34.2233 | 48.0144 | 53.7406 | 604.1841 |
| T-phase | 36.5050 | 39.3271 | 49.2769 | 50.6575 | 649.4655 |
| B-after | 40.5854 | 43.4167 | 69.7043 | 74.9872 | 739.0169 |

T-phase improves the descriptive tail over T-parent but worsens mean and
makespan. Preserve both, do not call it a general winner. D routing 48/47 is
not forced; P routing 32/63 must be interpreted using service/recovery costs
and release forecasts, not request-count balance alone.

The three-rate trace-conditioned diagnostic holds observed routes/cache/service
paths fixed. It quantifies causal/FIFO sensitivity and mismatch only. It is NOT
an online candidate evaluation; changing nominal offsets also changes real
cache/queue evolution, which this diagnostic intentionally does not predict.
Same-rate residual must be reported before treating a simulator as calibrated.
No candidate is promoted based solely on these fixed-path results.

Current operating boundary: edit/test/simulate in P's /root/pegaflow-hust;
D is queued, so no new real 2P2D pair is started. The recurring task stays paused.
Next: complete common native-worker integration, freeze a past-only policy-
feedback candidate and run three rates; then collect targeted real feedback.
