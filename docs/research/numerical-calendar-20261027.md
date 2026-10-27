# Exact calendar arithmetic: mechanism, then charged replay

Latest checkpoint is the final section below: corrected02 diagnostic complete;
fully charged02 running. Source01 failed activation and is preserved. Source02
is the minimal integration repair, not a retuned policy.

This continuation follows `forecast-feedback-20261027.md`. The previous P-head
revalidation01's18 CPU arms are complete and not promoted. No NPU run is implied.

## Observed defect and scope

An unchanged predicted worst request differed by one absolute-timeline ULP when
an unrelated stream changed iteration-coalescing horizons. The independent
two-stream reproducer fails with legacy grouped float addition, on Windows and
P/Linux, and is invariant without grouping. In the prior middle-default run a
2.84e-14s primary improvement overrode a materially worse secondary profile.
That is evidence of a numerical mechanism, not proof it explains all p99 losses.

The fix preserves each supplied binary64 primitive duration exactly. Every
finite binary64 number is an integer multiple of `2^-1074`. Define

`encode(x) = numerator(x) * 2^(1074-log2(denominator(x)))`,
`finish_ticks = start_ticks + repeat_count * encode(primitive_duration)`.

The event heap, additions, repeat products, horizon comparisons and integer
floor divisions use these integer ticks. Repeated iterations are encoded BEFORE
multiplication. The public endpoint rounds once to binary64. This is the exact
representation of the existing inputs, not a chosen physical time resolution,
gain threshold or fitted epsilon. No time/cost is deleted or penalized.

This does NOT claim exact physical estimates, exact arithmetic inside fitted
cost formulas, or an exact entire real-valued objective. Public endpoint and
profile subtraction remain binary64. The retained execution world remains its
original float event simulator; only candidate forecasting changes. Model,
resource/gate/cache/lease/complete-work and control-charging rules are unchanged.

## Implementation and limited validation

`binary_timeline.py` implements conversion. `schedule_batched(exact_timeline=True)`
keeps internal event time exact; default false retains old versions. The new
`NumericalCalendarSimulation` retains P-revalidation01 and its inherited D advice
and enables this forecast option via the snapshot. No new trigger or quota.

20 scoped checks pass locally; one existing explicit-input integration is
skipped and not counted as a pass. Tests include binary64 subnormal roundtrip,
independent-event invariance, the primary-tie/secondary-regression case and24
competing-gate/link/batch fixtures whose coalesced and uncoalesced outputs now
match exactly. Ruff passes. The first local test collection failed after import
sorting moved the overlay binder; the test import was repaired before freezing.

Source `exact-calendar-source-01.tar.gz` SHA
`aae8a6e171ab4ac245344a18e97faa119112dc696bb3e0c38271dfb525c0eace`,187 files.
Parent/reference `prefill-source-revalidation-01`; retained default remains
TimedMemo-normal-910B2. Frozen source is independent of the mutable research tree.

## Current run and next decision

P `/root/issue23-exact-calendar-diagnostic-pipeline-01`, detached PID/PGID298038,
launch1793067298.508838, supervisor4500s, maximum3 CPUworkers/4-core quota,
BLAS/OMP1. Runner `/root/exact-calendar-pipeline-01.py diagnostic`, SHA
`331ab2350bf4ae78082b9241f4f8ea31f5aa0a241b8ecc5a239b24ff91e60e65`.

Each world has native/reference/candidate x the three unchanged rates,95 requests,
same input/seed/cache budgets. Default sharing0 then unmeasured sharing1 sensitivity.
This first replay explicitly fixes measured controller CPU to zero in ALL arms
to separate numerical decisions from measured-control jitter. RPC and modeled
physical stages remain. It is a mechanism diagnostic, NOT net TTFT performance;
no improvement from it may be advertised as fully charged performance.

After completion verify full work/source/input/history audits and inspect
same-state/same-request route/advice changes. Then use the SAME frozen source in
separate fully charged default and sensitivity matrices, with actual forecast,
fit, inventory, routing/advice/commit CPU on the existing control lane, all
H2D/copy/save/decode/drain retained. Preserve every outcome. A correct numerical
fix can still increase net p99 through its CPU cost or change cache trajectories.
Do not patch an active frozen run or select whichever reference run is favorable.

Current checkpoint is diagnostic running, charged replay not yet started.
Use actual process/result/complete files, not this predicted state. Hourly work
remains ACTIVE, no commit/push, no real parallel serving validation or promotion.

## Activation failure01 and corrected02 (supersedes running01)

Pipeline01 finished all18 diagnostic arms,554s, exit0/no supervisor stop, with
source/work/history audits passing. However it did NOT activate the numerical
fix. `ControlReleaseSnapshot.configure_projection` returns a new dictionary and
does not call the base configuration; this dropped the option set by the first
implementation. The equal original forecasts/execution paths prompted inspection.
An original-input one-request test independently observed0 enabled calendars out
of68. The candidate label in01 is contradicted by this activation failure; its
results are not accepted as corrected-calendar evidence or a performance gain.
The direct arithmetic fixture remains valid, separate evidence.

Original full archives/root+local SHA verified and retained:

- default01 `63512cb28508f0b6593673567f5ffaf4634f2ff33ed06d2e00cd98deb9643b28`
- sharing01 `b1eed9a48f7246ec770716c82f3136c89693246e462acdf030d1b73d1f7ae080`

No charged01 stage ran. Separate `exact-calendar-activation-failure-01.json`
qualifies the old results without modifying their raw successful exit codes.

Source02 sets arithmetic at the actual calendar-call boundary after all phase
configuration overrides. Calendar output identifies its applied arithmetic;
every selected initial quote retains that fact. The runner and an independent
audit require95 actual exact candidate forecasts and95 float reference forecasts
per rate; a label alone cannot pass.18 local tests now pass including this real
input/isolated-seed invocation, with no skipped activation test. No native build.

New immutable source189 files SHA
`1a3be47bff9d6e5867727c0ae51a70b95febd07ef078ced3cd8168b371f95261`.
`/root/exact-calendar-pipeline-02.py diagnostic`, runner SHA
`9624dc22ce32368a7c03d57e924a750e128f706490f57b944bde51b67e2de48c`,
PID/PGID309782, launch1793068116.611992,4500s/3workers/OMP-BLAS1.
Current root `/root/issue23-exact-calendar-diagnostic-pipeline-02`; source frozen
at `/root/issue23-exact-calendar-source-02`. No charged02 stage started yet.

Local independent arithmetic fixture alternating9 repeats measured median
wall0.04374→0.05235s (about20% extra), including both grouped/un-grouped diagnostic
calendars. This is a Windows microcost, not projected end-to-end cost or NPU time.
The fully charged replay must measure actual additional CPU. The source02
same-round reference uses the shared controller implementation with arithmetic
off; it is not a byte-identical reexecution of old source01 or an old baseline.

Next: finish02 diagnostic and activation/source/work audits, inspect path
differences, archive then start the same frozen02 fully charged three-rate
default and sharing sensitivity. If a real bug is found preserve evidence and
repair minimally; never reinterpret failed activation as passing. Default
TimedMemo and previous partial-success versions remain retained. Hourly ACTIVE.

## Corrected02 diagnostic complete; fully charged02 now running

Diagnostic02 finished1793068678.946324 in562.135s,17 steps exit0/no stop.
All18 arms pass source/input/history/control/resource/complete-work audits;
570 actual selected forecasts per world are independently checked (candidate
exact, reference float). Both original-root and local full archives are verified:

- default02 SHA `c605c3eb998666f4642590db6f17f76c8d3c70ef76a0733d600ba66ab8541492`
- sharing02 SHA `88e312143d21190db239c97172b65aedd1c9955fe5a1977468b781e4d8d4becc`

`exact-calendar-diagnostic-summary-02.json` SHA
`1430927f78877b91a68fd63516849e03e7943c89c0f83142548abe318640d770`
contains mean/p50/p90/p95/p99 and all original archive references. These numbers
fix measured CPU to zero, retain RPC/physical work, and are NOT net gains:

| Scenario | Low diagnostic p99 reduction | Middle | High |
| --- | ---: | ---: | ---: |
| Default sharing0 | 8.64% | 10.52% | 1.83% |
| Unmeasured sharing1 boundary | -3.42% | 6.17% | 21.26% |

Corrected02 differs from01's actual-disabled path. In default-low the first
different initial command is dna-insert:2 at the same origin5.303210217 and the
same latest completed history. P0→P1 retains640 prefix tokens and D0; predicted
Dfirst is nearly tied while P release differs. Actual PAPI0.31514→0.22847s and
D admission interleaving subsequently changes. Middle/high also change D
recovery boundaries BEFORE their first differing initial route command. For
example middle's first different batch trace at46.7096s replaces zero-prefix
restore with a handoff followed by0.41619s restore for git-multibranch:3.
Do not label these differences loss of accepted cache or RDMA saturation.

New alignment artifacts retain first differing resource/batch records and pair
every request by ID. The first helper incorrectly asserted completion-list order
was equal after policy changes; that offline assertion failed without writing a
report. It now checks equal unique95 IDs and reports ordering changes separately.
No input/output/source audit or tolerance was relaxed. The corrected offline
helper is outside frozen02 execution; its updated hash is in the handoff.
These traces support arithmetic→decision→cache/queue feedback, not attribution
of every tail delta to one action. No P prefix changes occurred in diagnostic02;
sharing-low made2 P decisions and kept both. D advice effects and initial routing
must not be mislabeled as P-revalidation benefits.

The same immutable source02 is now running with ALL actual control costs:

- command `/root/exact-calendar-pipeline-02.py charged`
- root `/root/issue23-exact-calendar-charged-pipeline-02`
- PID/PGID320616, actual launch1793068869.052397, supervisor4500s
- default0 then sensitivity1, each native/parent/candidate x3 rates,95 requests
- max3 workers,4-core quota,OMP/BLAS1; no simultaneous matrix or GPU work
- source SHA `1a3be47bff9d6e5867727c0ae51a70b95febd07ef078ced3cd8168b371f95261`
- runner SHA `9624dc22ce32368a7c03d57e924a750e128f706490f57b944bde51b67e2de48c`

Next hourly continuation must check the actual result/process/log and avoid
duplicating this pair. After completion verify/source-audit every arm, archive
both complete outputs+SHA locally, inspect `numerical-feedback.json` and original
per-request paths, actual CPU, P/D advice and first divergences against each
SAME-ROUND parent. Extra arithmetic CPU can offset numerical decision benefits;
do not subtract it or quote the diagnostic as net performance. Preserve losses
and01's failed activation. No version is promoted and no NPU readiness is claimed.
If evidence motivates another fix, freeze an independent version; do not modify
this running source. Research remains ACTIVE hourly, no commit/push.

## Fully charged02 complete: partial gains, not promoted

This checkpoint supersedes all running statements above. Pipeline02 finished
1793069483.646606 (2026-10-27 10:51:23 Beijing),614.492s,17 steps exit0,
no timeout/failure.18 CPU arms have complete source/input/work/control/history
and actual arithmetic-activation audits. Both complete original-root archives
were copied locally, their full SHA verified, and independently extracted:

- default `17978296fa1f8a13e6449b869ae183f38b5365b97f95f7ef3949bda8f1b657da`
- sharing `0575b1bd1ca69f109403e9486b7eee484e7ace897737c9fb16f2ad421ea1b479`

Evidence `evidence/forecast-feedback-20261027/exact-calendar-charged-summary-02.json`
SHA `1b16c4ac8b36c0fc077e19c0d6dfc13c257b95df117dc92252157a5f303fcb06`
retains all metrics and raw paths. Separate charged alignment artifacts pair
all95 IDs and check stage accounting. They do not relabel diagnostic results.

| Scenario | Rate | Parent p99 (s) | Candidate p99 (s) | Reduction |
| --- | ---: | ---: | ---: | ---: |
| Default sharing0 | .2452 | 17.2599 | 15.8585 | 8.12% |
| Default sharing0 | .4903 | 18.6792 | 17.1106 | 8.40% |
| Default sharing0 | .9806 | 18.7381 | 19.0358 | -1.59% |
| Unmeasured sharing1 boundary | .2452 | 19.8439 | 16.4077 | 17.32% |
| Unmeasured sharing1 boundary | .4903 | 20.5150 | 21.7734 | -6.13% |
| Unmeasured sharing1 boundary | .9806 | 20.0565 | 20.2422 | -0.93% |

Reference is this same round's prefill-source-revalidation parent, not the old
TimedMemo run or a selected historical baseline. Measured route/boundary/model/
advice/iteration control, modeled RPC/H2D/recovery/save/copy, waiting and full
drain remain charged. Results are CPU simulation, not real NPU evidence.
95 nearest-rank p99 is max; single exposed replay is not stable significance.
Default TimedMemo remains retained. No version promoted or committed/pushed.

Observed behavior and its limits:

1. Default-low mean7.1055→7.6201s and makespan508.416→532.991s worsen despite
   p99 improving. Only33/95 requests improve. This is a tail-specific tradeoff,
   not an overall-speed claim. Middle44/95 and high41/95 requests improve.
2. Default-high candidate worst feal:12 worsens18.1345→19.0358s for that same
   request: P API+0.1183s, P→D+0.8934s, before-P−0.0353s, Dfirst−0.0751s.
   Its actual P prefix changes5248→4992; it is not a fixed-byte hardware test.
   The pair's max changes identity (parent feal:13). Do not equate the matched
   delta with the difference between maxima.
3. Default-high fix-git:14 has identical P API5.29388s but ready452.591→362.676s.
   P→D waiting grows1.115→9.456s. Conventional TTFT7.291→17.351s worsens while
   nominal latency370.593→290.739s improves. Earlier causal ingress into a
   different busy queue explains the coexistence; it does not justify changing
   the primary metric or subtracting the wait.
4. Sharing-middle candidate worst git-multibranch:12 grows10.4845→21.7734s.
   Before-P grows0.8047→12.5225s while P API improves8.5440→8.1068s. Candidate
   route P1 has more prefix (2048 vs640 on P0), but P1 is executing
   distribution-search:12 until201.848s then feal:12 until212.626s. Own P API
   begins212.626s. Actual first221.8778 is close to original forecast222.2612.
   Thus this large regression is not evidence that the selected forecast
   omitted H2D/queueing. All alternative forecasts were not retained, so it
   does NOT prove that P0 was feasible/better under the pending-global objective.
5. Default-high route control63.421→72.663s accompanies630→673 full calendars;
   sharing-middle62.074→74.810s accompanies613→689. Both changed search work
   and arithmetic overhead are plausible contributors, not isolated causes.
   Aggregate wall/full-calendar includes refresh/pruning and is not a calendar
   microbenchmark. Never add these already charged totals to TTFT again.
6. No P revalidation action occurs in any candidate arm (sharing-middle has
   one keep call). D move counts default3/0/0, sharing1/0/1. Numerical routing,
   D-boundary selection and CPU→queue/cache feedback explain the changes under
   consideration; do not claim a benefit from unused P revalidation or invent
   RDMA congestion. Zero-control wins cannot predict the charged signs.

Next bounded work: replay identical original-input forecast states for CPU
profiling of float/exact arithmetic, count calendar/primitive conversions and
cache hits, then capture all candidate pending-first profiles at the P-queue
divergence above. Keep live state/history fixed inside a diagnostic and label it
mechanistic, not a new policy effect. Only if a measured redundant computation
or a demonstrated objective/feasibility mismatch is found should the next
candidate change. Do not add a time epsilon, force balancing, widen triggers,
or consume future realized timings. Preserve numerical correctness even if
performance optimization needs another step. Hourly research remains ACTIVE;
there is no active matrix after charged02 and no fresh NPU availability proof.
