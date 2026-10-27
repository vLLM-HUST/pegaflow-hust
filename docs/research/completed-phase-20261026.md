# Completed exclusive D-phase feedback: useful mechanism, unstable tail gains

Candidate `completed-exclusive-D-01` is retained, **not promoted**. Its first
three-rate CPU comparison improved conventional TTFT p99 at all rates; the
reversed-order confirmation did not reproduce that result. No NPU experiment,
native-engine change, automatic timer, or remote main push was performed.

## Why this change was attempted

The previous model correctly excluded batched D API residence from singleton
service fitting: residence includes other requests and queue waits. It also
discarded independently observable completed exclusive restore/finish intervals.
That left those resource costs fixed at the initial calibration even when
same-arm completed information was available. The candidate repairs this
information path without training queue wait as compute or assuming RDMA
congestion.

For D node n, retain the original nonnegative physical model
`restore_n(x)=a_n+b_n*x*bytes_per_token`. Seed it from the original isolated
calibration and append completed owned restore samples. Update finish cost with
the seed plus completed sample mean. Keep singleton prefill/decode compute fits
unchanged. Replace the old component inside aggregate first/tail predictions;
do not add a second restore/save penalty. Full decode, leases, capacity,
source/destination gates, cache evolution and control release stay unchanged.

Only completed iteration events can supply new samples. Each phase must have
one request owner, a matching controller-cost event and a completion before the
prediction cutoff. Subtract separately charged controller CPU once. In this
simulation those events stand in for future serving instrumentation; they are
not new hardware observations. The policy does not read `physics()`, residual
sample IDs, or future scheduled phase durations. Model maintenance is measured
and delays the controller timeline, following the retained fitting convention.

## All results, including confirmation failure

Both matrices used the same frozen candidate, original 95 requests / 8 causal
sessions, three Poisson arrays, calibration, 910B2 device-KV scenario, 2 GiB host
pool per instance, four-sequence D model and joint-phase residual world. Shared
compute fraction remained zero; no unmeasured batch speedup was introduced.
The reference is `control-visible-work-01` in the same world, not a different
historical baseline. Routing, boundary, iteration and fit CPU are charged.

| requests/s | First reference → candidate p99 (s) | Reduction | Reversed reference → candidate p99 (s) | Reduction |
|---:|---:|---:|---:|---:|
| 0.24516 | 32.030 → 29.592 | 7.61% | 32.190 → 32.203 | −0.04% |
| 0.49032 | 31.705 → 27.769 | 12.41% | 32.281 → 32.343 | −0.19% |
| 0.98065 | 32.265 → 27.385 | 15.12% | 28.251 → 32.029 | −13.37% |

Both comparisons passed nine-arm accounting and 285 completed-phase label
checks. Model-maintenance CPU was 0.249–0.320 seconds per candidate arm, already
included in elapsed simulation time. This does not include all cost of using
the new model inside search; routing/boundary wall includes that cost separately.
Ninety-five observations make nearest-rank p99 the maximum; these are exposed
development replays, not significance or stable-tail estimates.

## What the request-level evidence says

The first run's high-rate gain did **not** come with lower overall endpoint MAE:
MAE rose from 1.183 to 1.669 seconds. Conditional on the candidate's actual routes
and accepted prefixes, predicting at the observed D API start, online restore
updates also did not consistently beat the initial fit. D0 restore MAE changed
0.679→0.721 / 0.627→0.634 / 0.497→0.498 seconds across rates; D1 changed
1.004→1.006 / 0.952→0.948 / 1.032→1.053. This fixed-trace diagnostic evaluates
prediction, not an alternative policy outcome. More observations alone do not
establish better decisions in a world whose residual distribution is seeded
from the same small calibration.

The reversed high-rate worst candidate request, `git-multibranch:11`, had no
accepted D prefix in either arm. Its D API-to-first interval rose from 10.744
to 18.237 seconds. Intervals of other requests' compute overlapping that wait
rose from 2.545 to 9.958 seconds; save overlap rose 0.162→0.250 seconds. Own
restore was zero apart from controller CPU. The candidate initially predicted
first at 386.636 seconds and observed 386.652: it accurately predicted this
particular bad outcome. Therefore another restore penalty or claiming pure
H2D prediction error would not explain or fix this tail.

These overlaps are interval accounting, not independent causal contributions;
mixed compute intervals can contain multiple jobs. The high-rate candidate's
total routing+boundary wall was lower (106.915 vs 124.216 seconds), yet p99 was
worse. Neither total CPU reduction nor phase-fit accuracy is sufficient. The
admitted-work objective, subsequent arrivals/session release, cache changes and
competition between full decode and pending prefill all remain relevant.

At 12–17 of the 95 decision origins per arm, an exclusive restore/save interval
was already in flight. The calendar conservatively restarts an acquired
iteration rather than independently modeling its elapsed remaining phase.
This is a concrete next modeling hypothesis, **not established as this run's
root cause**; the cold worst request above had an accurate endpoint prediction.
Any age-conditioned repair must use observed start/progress and completed
durations, not the execution world's future completion timestamp. Compare it
under the same charged world and retain all reversals.

## Reproduction and preserved evidence

Source archive `completed-phase-source-01.tar.gz`, SHA256
`5c000d44159e35e9dd15a050cf45f25e230c432a0f1d07e5b336c9c10565b610`.
P frozen source is `/root/issue23-completed-phase-source-01`; results are
`/root/issue23-completed-phase-01` and `-02`, with complete independent SHA-verified
archives locally and under P `/root`. Compact evidence and source receipts are
in `docs/research/evidence/completed-phase-20261026/`.

```bash
PYTHONPATH=python:experiments/issue23_strategies \
  python experiments/issue23_strategies/run_pcie_matrix.py \
  --repo "$SOURCE_ROOT" --inputs "$INPUTS" --seed "$CALIBRATION_SEED" \
  --output "$FRESH_OUTPUT" --policy completed-phase --workers 3 \
  --residual-world --phase-residual-world
# Use another fresh output plus --candidate-first for the retained confirmation.
```

Use `analyze_control_work.py` with the matching source receipt for accounting,
`audit_completed_phases.py` for owned phase/CPU reconciliation, and
`analyze_completed_phase_feedback.py` with the original seed for the fixed-trace
prediction diagnostic. No task/rate discriminator, gain threshold, fitted
balance quota or new initial calibration was added. The default retained parent
remains `TimedMemo-normal-910B2`; this candidate is an explicitly partial-success
branch pending more specific modeling and instrumented real feedback.
