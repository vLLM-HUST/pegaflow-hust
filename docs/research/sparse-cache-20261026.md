# Evidence-driven cache projection optimization (2026-10-26)

This is a CPU-only continuation of `cache-evolving-batch-01`, not a new NPU
serving result. The retained parent and all of its mixed/negative outcomes remain
available. Scheduled monitoring remains paused.

## Observed bottleneck and hypothesis

An instrumented complete causal replay profiled routing and D-boundary decisions
32 through 39. `sync_protection` was called 6,684 times, with 9.32 seconds of
inclusive profiler time. It rebuilt every physical-allocation dataclass on every
lease transition, even when the protection flag did not change. The whole sample
contained 821,365 dataclass replacements. These instrumented times identify work;
they are not latency comparison results or an estimate of PCIe/DMA utilization.

The hypothesis is specific: preserving unchanged immutable allocations can reduce
the CPU cost of the more faithful cache-evolution forecast, without sacrificing
its owner-closure, publication, reclaim or resource constraints. No additional
task/rate threshold, fairness quota, restore penalty or RDMA-congestion assumption
is introduced.

## Exact representation change

For physical allocation `u` on node `n`, the protection invariant is unchanged:

```text
protected'(n,u) = intrinsic(n,u) OR lease_count(n,u) > 0
```

`sparse-cache-protection-01` creates a new immutable object only when that Boolean
changes. An unchanged node retains its exact tuple identity; a changed node keeps
the original allocation order, identity, keys, payload and charged bytes. Existing
decision-local indexes can consequently be reused. Equality is never reduced to
a hash-only comparison. Source and destination leases still protect the same
allocations until their actual owners release them.

The optimization objective, route ordering, 64-candidate budget, service fits,
causal arrival stream, four-sequence D capacity, cache publication/reclamation and
execution world remain identical to the parent. Lower CPU time is measured and
charged to request execution; it is not subtracted from reported TTFT after a run.
It can change subsequent ready times, concurrent requests and cache histories,
so lower planner CPU alone does not prove lower closed-loop p99.

## Evaluation contract

1. Differential tests include overlapping owners, intrinsic protection and
   independently published allocation identities.
2. Three full 95-request replays with identical zero control cost must produce
   identical requests, stages, resource events, D iterations and original cache
   forecasts. This is a mechanism-equivalence diagnostic, not net performance.
3. Restore actual CPU accounting for three-rate comparisons, then reverse the
   reference/candidate execution order for a confirmation. Preserve every result.

Rates are 0.2451617077, 0.4903234154 and 0.9806468308 requests/s, same seed, eight
causal sessions and original isolated calibration. The existing 910B2 memory
budget, 2 GiB host pool per instance and no unmeasured batch-compute speedup remain
unchanged. Conventional TTFT is causal-ready to first token. With 95 requests,
the nearest-rank p99 is the maximum; these are descriptive development replays.

## Results

All 285 equal-control requests matched exactly, including original cache quotes.
The two measured matrices each passed the independent nine-arm accounting check.

| Rate | Round 1 parent→sparse p99 (s) | Round 2 parent→sparse p99 (s) |
|---|---:|---:|
| 0.2452 | 28.543→28.957 | 32.482→32.124 |
| 0.4903 | 31.595→32.586 | 38.985→32.489 |
| 0.9806 | 32.636→32.476 | 31.434→49.707 |

Round 1 ran parent before sparse; round 2 reversed them. Routing plus boundary
wall decreased by 23.4%/23.1%/13.9% and 35.8%/51.7%/22.4%, respectively. These
whole-replay reductions include changed subsequent work and are not isolated
per-operation microbenchmark speedups. The severe high-rate tail regression in
round 2 is retained. This is useful implementation work, **not a promoted p99
winner**; the default reference remains `TimedMemo-normal-910B2`.

### Why the high-rate tail deserves another model correction

The round-2 candidate's `dna-insert:11` took 49.707 s, versus 24.784 s for the same
input under its parent. It accepted more cache, yet its path was:

| Component | Parent (s) | Sparse (s) |
|---|---:|---:|
| Causal ready to P API start, including control/queue/copy | 4.606 | 21.334 |
| P API | 4.400 | 0.608 |
| P completion to D API start | 0.836 | 14.160 |
| D API start to first token, including batch interference | 14.943 | 13.605 |

Its original predicted P completion was 375.289 s versus actual 375.355 s; the
first-token prediction was 379.365 s versus actual 403.121 s. Thus this request's
large endpoint residual cannot be described simply as a bad P-queue prediction.
Its history/handoff network service totaled approximately 0.208 s and software
direction-link wait was zero. D control occupied 375.355–378.229 s with no device
gate. In that interval, P1 admitted `git-multibranch:12` (375.355–384.187 s), then
`headless-terminal:11` (384.187–389.476 s). The handoff acquired its device gates
at 389.513 s. The original forecast omitted D-control delay and could project a
handoff before that intervening P work.

This is observed simulator evidence, not real hardware telemetry or proof that
one omitted term explains all regressions. Actual D acceptance also changed from
the original 9,472-token quote to 7,040; future not-yet-admitted work remains a
forecast limitation. The next candidate therefore tests controller release times
explicitly instead of merely adding a final scalar latency penalty.
