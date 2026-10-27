# Controller completion in the resource calendar (2026-10-26)

Candidate: `control-release-calendar-01`, parent: `sparse-cache-protection-01`.
This CPU-only candidate follows the recorded blocking path in
[the sparse-projection review](sparse-cache-20261026.md). It changes the forecast;
the execution world, measured-cost accounting, candidate budget and workload do
not change. It is not an NPU validation.

## Missing term and model

The execution model already charges route CPU/RPC and a D-boundary control
operation. The old batch predictor could schedule the candidate P request
immediately and omit D control. These are release constraints: during control,
another request can acquire a device before the intended cache handoff does.

Let `t` be the current routing decision origin (after external/router waiting),
`r_P` and `r_D` the retained calibrated control RPC constants, and `H_P(t)` and
`H_D(t)` the same-arm CPU observations whose modeled control completions are no
later than `t`. The CPU estimate is the unweighted sample mean; it is zero before
any completed observation. No task, rate, gain threshold or fitted multiplier is
used.

```text
mu_X(t) = sum(cpu_i for i in H_X(t)) / |H_X(t)|, or 0 if H_X(t) is empty
candidate_P_enqueue = t + r_P + mu_P(t)
D_control_complete = D_slot_admission + r_D + mu_D(t)
copy_start = earliest time >= D_control_complete with all required resources free
```

The P candidate enters FIFO only on predicted routing completion; it does not
occupy a P slot while its controller is running. D control reserves the same
sequence slot as execution but has an empty device-resource set. Source/destination
device gates and directional-link availability still determine copy admission.
An already issued D-control operation uses its observed start and known issued
duration to predict its remaining interval. Current-decision CPU and future
completed measurements are never supplied to the online estimator.

The event calendar includes controller-release events as coalescing boundaries,
so token iterations cannot jump over a newly eligible stage. Physical cache
publication, reclaim, lease protection, H2D, full decode and synchronous save
remain modeled. The objective remains conventional causal-ready-to-first TTFT,
with all actual planning/migration costs charged once.

## Scope and falsification

Use the original three arrival arrays and 95 causal requests. The initial service
seed, 910B2 memory budget, D concurrency and conservative batch scaling remain
unchanged. CPU means reset per arm; no baseline measurements initialize treatment.
The controller constants are the same retained replay input, not refitted here.

Tests distinguish controller waiting from device ownership, verify that a delayed
P request cannot block an already admitted request, and exclude unfinished CPU
observations. Full runs must additionally audit the original quote's observation
cutoff. A smaller forecast residual is not sufficient: retain/reject based on net
tail behavior and analyze all negative outcomes. Future, unadmitted arrivals and
after-P boundary changes remain uncertainty; this is not an oracle calendar.

## Completed charged results

| Rate | Parent→candidate p99, round 1 (s) | Parent→candidate p99, round 2 (s) |
|---|---:|---:|
| 0.2452 | 31.373→30.312 | 27.992→24.758 |
| 0.4903 | 32.092→31.804 | 32.533→35.585 |
| 0.9806 | 24.878→32.068 | 31.573→32.647 |

Round 2 reverses the arm execution order using the same immutable policy/source
files and the frozen runner's `cell` function; its separate orchestration script
is saved and hashed in the result. No service/cost/arrival parameter was changed.
All actual CPU overhead is included. This is **not a promoted three-rate winner**.

Round-1 first-token endpoint MAE was 1.070→1.123, 1.333→1.367 and 1.269→1.235 s.
The high-rate candidate's worst request, `feal-differential-cryptanalysis:12`, had
predicted first 379.988 s and actual first 379.934 s. Its TTFT was still 32.068 s:
predicting that particular completion correctly does not prove that the chosen
route makes the entire closed-loop replay optimal. Compared with the same input
under the parent, P service was identical, but D API-to-first increased by 6.066 s
(including batch interference), while its D route changed from D0 to D1. These
are different online states, so this comparison alone is not a same-snapshot
counterfactual demonstrating that D0 would have been preferable.

The formerly observed `dna-insert:11` is faster in this replay, but its ready
time, concurrent requests and cache state also changed. It must not be advertised
as an isolated causal speedup from the release constraint. Both matrices and their
regressions remain archived. A separate fixed-control diagnostic records completed
candidate scores to investigate the current-live tail objective versus the new
request's own first-token time; its latencies are not net performance evidence.
