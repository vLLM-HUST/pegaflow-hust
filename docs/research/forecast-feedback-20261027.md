# Forecast-control and cache feedback investigation — 2026-10-27

Current continuation is `numerical-calendar-20261027.md` and the version registry.
All running/next-run paragraphs below are historical checkpoints unless explicitly
confirmed by that newest entry; do not relaunch completed pipelines.

This continues the hourly research task; no NPU availability is assumed. Retain
TimedMemo-normal-910B2 as the default and slot-release-first-01 as a default-world
CPU candidate. Strong-sharing co-batch losses remain failures, not replaced results.

## Completed alignment

`analyze_release_feedback.py` aligns all 95 requests in each of the three completed
release-cobatch-sharing-01 comparisons. It preserves original route commands,
original effective-cache predictions, actual acceptance, stage paths and search
work. The new report is `cache-profile-20261026/release-feedback-alignment-01.json`.

In the middle case, total calendar bound checks increased 615836 → 746161 while
completed calendars increased 842 → 889. Route commands differ on 63/95 requests.
The first command difference is already `fix-git:3` at time 25.1774 s, before the
candidate's first actual release selection at 49.9707 s. This does **not** prove
CPU alone caused the difference: a prospective calendar can predict a release
before the world executes one. Both forecasting semantics and measured control
cost can affect later routing, cache contents and causal admission.

For `headless-terminal:13`, the candidate commanded zero P prefix and its shadow
also accepted zero; no accepted-cache-loss claim is justified. Its original
forecast had only two completed calendars and 43 pruned calendars, versus 14 and
36 in the parent. The particular tail decision was cheaper even though the
candidate's aggregate control computation was more expensive. Optimizing only
this request's route CPU would therefore miss the feedback chain.

## Bounded profiling diagnostic

Source01 SHA `95d3f52dce2270e623c51c729f06314ba3ee595d01bc5b7531e206716d0ee869`
failed after 99.912 s, exit1, without timeout. The diagnostic tried to read a
closed detached-decision scope after choose returned. This is an instrumentation
bug, not a policy or serving failure. Original source/output/log remain on P.
Source02 captures statistics inside the live forecast callback; policy code is
unchanged. SHA `088cd63c73b97cae2059b763185a3c9edad9f96af3efa1e71392b769f0a42727`.

Command: frozen source02 `profile_release_forecast.py --inputs` the original
simulation-inputs.json `--seed` original calibration-seed `--output
/root/issue23-forecast-profile-02`. Supervisor `/root/forecast-profile-runner-02.py`,
PID 4113586, started Unix1793039263.7437184, limit900s, one CPU worker and BLAS/OMP1.
No matrix runs concurrently. Zero measured-control cost is used only to obtain a
reproducible causal state for profiling. This is not a net performance comparison
or an exact reconstruction of the previous measured CPU timeline. Full workload,
cache evolution, physical work and completed-only model updates remain active.

Source02 completed exit0 in 108.407 s without supervision stop. The complete
95-request diagnostic and failed01 logs are archived together, original P/root
and locally SHA verified: `9842b7b825d801ca839f67722d579fabef21a512b63fbff26685299bfa99f393`.
No net performance is inferred from this diagnostic.

The target forecast ran 44 calendars, 1821 bound checks and 4888 batch begin calls.
Total profiled time was 0.6653 s. Protection synchronization consumed 0.1029 s
inclusive (15.47%), with 49993 physical lease-protection lookups. Release ranking
and co-batching were not invoked in these forecasts; removing release-choice
logging would not address this hotspot. Profiler instrumentation overhead is
included, so these are diagnostic proportions, not an isolated benchmark speedup.

At this new fixed-control state, available prefix tokens were 768/7680/11904/0,
and P1 chose 7680. This differs from the previous charged run and is not proof of
which original inventory update caused its zero-prefix command. Exact past
causal attribution still needs a richer inventory transition trace or replay.

## Candidate01 equivalence failure and minimal fix

The first delta-protection trial completed all raw arms but failed the mandatory
zero-control equality, exit1/no timeout after 255.163 s. The fully charged stage
was never started. Source01 accidentally inherited the earlier sparse projection,
omitting the retained parent's D_control admission/remaining-delay logic. This
was an implementation bug, not evidence for a different scheduling objective.
The new source derives directly from the full parent projection type. A targeted
check now compares new, active-control and boundary-revised D admission chains,
including ownership without a device gate. Seven checks pass on local and P.

Failed complete artifacts and pipeline logs remain local/P with archive SHA
`d29f5e75b56fd15df60fd9e4f721ce3b3020cc5e07c2ab0bce7ce6a292d85420`.
The old pipeline field did_not_change_policy_semantics was an intended scope,
contradicted by its equality failure; it is not a certification. Future receipts
use intended_equivalent_policy_semantics. No favorable failed-arm latency counts.

## Candidate02, completed recovery and charged comparison

`delta-protection-cobatch-02` retains its co-batch parent's selection and complete
projection, including D control. Lease counts crossing zero identify changed
physical identities; an unchanged immutable node tuple updates only those
identities. Copy/save/reclaim replacing the tuple forces full reconciliation.
Shared owners, intrinsic protection and held leases remain mandatory. New index
and dirty-set maintenance is measured, not free. No route quota or penalty added.

Source `/root/delta-protection-source-02.tar.gz`, SHA
`771bf0b7704732c91cd59d12e3b966d2fa435ef2169f01584b116d398c897fd1`.
Candidate01 source and outputs remain intact. The runtime is the unchanged stable
`/root/issue23-tools/pegaflow-joint-pipeline-01/.venv/bin/python`.

Original pipeline02 finished the three-rate zero-control simulation, then stopped
because the frozen package omitted offline audit helpers. This is a packaging
failure after simulation, not a failed policy equivalence. Recovery01 reused the
finished outputs without rerunning them, used a separately hashed audit kit, and
then ran the intended fully charged comparison with the **unchanged** source02.
Audit kit SHA `dbd87288c823be0273daf54bb6edb1d48340f2df3cd3fb5161d2fc96bedae7e7`.
Recovery supervisor `/root/delta-protection-recovery-01.py`, PID4157134,
started Unix1793042402.1794493, finished1793042711.4563637, failure=null.
Three workers, four-core quota, BLAS/OMP1, overall4500s/matrix1800s/audit120s.
No GPU work, build, cost refit, changed input or new seed.

All three zero-control rates have identical per-request, stage, resource-event
and iteration trajectories for parent and candidate. All18 arms across the two
matrices pass independent source/input/accounting/work-conservation and
completed-only release-estimate checks. This is tested equality, not proof for
all possible states. Full archives are retained on P/root and locally SHA verified:

- equivalence02: `1b1b5e34c97331850d70002d303c79113709b8236ec977813a0329c05bdf7a34`
- charged sharing02: `30346b691d7423d01f4beb6bd37009c2ae9aeeefdf527db01113f4a92bc146ea`

### Charged result: useful implementation candidate, not a new default

Comparison reference is release-preserving-cobatch-01, **not** TimedMemo or the
plain baseline. Both use sharing1, an unmeasured optimistic sensitivity boundary.
The full controller, model update, iteration, migration and final drain remain
charged. No overlapped control wall time is added a second time.

| Nominal rps | Parent p99 s | Delta p99 s | Reduction | Parent/candidate p95 s | Parent/candidate makespan s |
|---|---:|---:|---:|---:|---:|
|0.2451617|17.0863|15.3728|10.03%|15.7251 / 13.9290|419.130 / 419.057|
|0.4903234|17.3926|17.9855|-3.41%|15.3845 / 13.9617|285.371 / 278.188|
|0.9806468|18.3365|16.6923|8.97%|13.2308 / 14.5143|258.732 / 263.786|

Middle p95/mean and makespan improve while p99 worsens; high p99 improves while
mean/p95/makespan worsen. Retain all outcomes, no promotion. With95 requests p99
is the maximum; these are single development replays, not robust significance.

### Per-request and search-work explanation

In the middle charged pair, the candidate's new maximum is headless-terminal:12.
For that **same request**, parent→candidate TTFT15.3845→17.9855s decomposes as:

- before P API:3.9325→8.6516s (includes control, copy/gates and waiting; not pure FIFO);
- P API:1.3213→5.1662s, accepted P prefix9600→1280 tokens;
- P end→D API:2.4850→3.8279s;
- D API→first:7.6457→0.3398s, route D1→D0.

Thus a better D-side path is outweighed by a worse P-side path. Both initial
commands already request the corresponding9600/1280 P prefix. Do not call this
loss of an accepted cache allocation, or attribute it to network congestion.
The parent's worst feal-differential-cryptanalysis:11 instead improves
17.3926→11.9826s, with P prefix640→11776 and P API9.8440→4.6978s.
The critical request changes; optimizing one prior worst request is insufficient.

Aggregate middle boundary CPU36.7040→45.3321s (+23.51%), but boundary bound checks
also rise240517→299200 (+24.40%); full boundary calendars273→287, pruned2733→3246.
Route CPU71.5693→70.6923s despite route checks611906→586530. These counts support a
changed-workload explanation for part of the control increase, but are not a
fixed-state causal isolation or a valid seconds-per-check cost estimator.

`compare_control_perturbation.py` also aligns zero-cost and charged runs of the
same frozen package. Middle p99 changes37.4441→17.3926/17.9855s, and69/95 requests
change route or accepted cache in each arm. Removing overhead changes the causal
state trajectory; its outcome cannot be obtained by subtracting CPU from TTFT.
Do not interpret the diagnostic as a recommendation to add artificial delay.
Original commands, forecasts and actual accepted bytes remain separately recorded.

### Paired mechanism diagnostic: current03, no new policy matrix

Diagnostic01 failed after91.310s while copying a live model's RLock; diagnostic02
failed after106.283s on MappingProxyType in a warm projection index. Both exit1,
no supervision stop and no usable paired timing. These are offline instrumentation
failures, not policy/serving failures. Full failure source/logs/outputs on P and
local SHA `ca48b5e24383259ed657a04cbb02d5885e3438b6f7b56c01e916a659e6d7fefc`.

Corrected diagnostic03 excludes the live model/locks/callbacks, shares read-only
frozen costs, and preserves physical tuple identities while separately cloning
mutable index/counter state. Blind deepcopy would have invalidated warm memo
identities and biased the timing even if it had not failed. Five local focused
checks pass, including warm payload indexes/independent counters and the existing
protection contracts. This does not change the policy or candidate02 source.

Current `/root/issue23-paired-protection-profile-03`, supervisor
`/root/paired-protection-runner-03.py`, PID72526/child72527,
started Unix1793050734.2330651,900s limit, one CPU worker/BLAS1.
Source SHA `302af9006ffa3806fd2ed934da2dfc5b236291a66d1ded124df73d2cc2cbdf8c`.
Completed exit0/no stop in158.858s,167 paired calendars; source/95-request/stage/resource/iteration equality to the unchanged zero-control parent passed. Full local/P archive SHA `a4a4a496e19edc3f42f4a6f4e7cedd0240d1bbc628f6544b6e7ac7b3977b5d28`.

It uses the original causal replay with zero measured control cost to capture
route and D-boundary calendars for headless-terminal:12 and:13. Each calendar
receives fresh identical detached state/arguments in alternating parent/delta
order; snapshot cloning is outside measured calendar CPU. Full predictions and
pruning counters must match each other **and** the untouched original invocation.
Profiling adds a separate instrumented pair. This isolates implementation cost
at sampled states, not net gains or reconstruction of the charged run.

`analyze_paired_protection.py` completed against the original zero-control parent.
At104 route calendars, unprofiled paired CPU(two repeats) is1.43646→1.48169s
(+3.15%); at63 boundary calendars3.54478→3.60734s(+1.76%). These sampled identical
states support a small implementation slowdown, not the hypothesized speedup.
Nested instrumented sync_protection grows0.21831→0.31210s in routes and
0.10094→0.15732s in boundaries. Indexed sync self time roughly doubles; dirty
acquire/release tracking adds cost too. Profiling and unprofiled totals remain
separate. Two target requests do not characterize every state or real hardware.

This resolves part of the ambiguity: aggregate middle boundary CPU grew largely
alongside more searched work, but the primitive itself also did not repay its
maintenance at the sampled states. Retain candidate02 and its favorable tail
samples without promoting it. Do not call those gains proof of faster control.

### Evidence-directed follow-up: dirty-scan-cobatch-01

Remove the physical-position index, retain zero-crossing dirty lease identities.
For a new immutable tuple use the parent's exact full reconciliation; for the
same tuple with an empty dirty set do nothing; otherwise scan identities and
recompute protection only for dirty owners. Publication, intrinsic guards and
shared-owner leases stay mandatory. This is still O(N) when dirty; no asymptotic
or p99 win is asserted. The measured index overhead motivates this change,
not a fitted trigger, gain threshold, task special case or forced balancing.

Frozen source `/root/dirty-scan-source-01.tar.gz` SHA
`cac23a4f0a0ac46ebefe8c99ea100d1f28dc2319d9df6bf2d08ec39e0ad61741`.
Eight focused local checks pass, including protection, D-control preservation,
co-batch semantics and warm diagnostic index isolation. Source includes all
required offline audit helpers. No GPU code, workload, rate, cost seed or budget
changes; parent remains release-preserving-cobatch-01 in both pairs.

Actual supervisor PID79235 started Unix1793051121.823283; matrix controller79237 and three workers79238/79239/79240 were verified under the frozen source. No final result at launch verification.

The new bounded pipeline `/root/dirty-scan-pipeline-01.py` first runs the three
frozen rates under zero-control equality, then independent source/work/release
checks. Only on success does it run the fully charged sharing1 sensitivity.
Outputs `/root/issue23-dirty-scan-equivalence-01` and
`/root/issue23-dirty-scan-sharing-01`; pipeline receipts under
`/root/issue23-dirty-scan-pipeline-01`. Three workers/4CPU,BLAS1,4500s total,
1800s per matrix/120s per audit. Actual launch/start receipt is authoritative;
no current result claimed. Default sharing0 is not automatically appended.

Next hourly continuation: inspect pipeline result/log/process and completed
markers; no duplicate launches. Fetch full archives/SHA and align all95 requests.
Check whether removing the index reduces work cost and whether state feedback
still shifts the maximum adversely. Preserve every outcome and use evidence for
the next hypothesis. Keep hourly research ACTIVE; no NPU claim, commit or push.

## Latest continuation: dirty scan completed; causal D-binding hypothesis

This section supersedes the prior running/pending statements above. Dirty-scan
pipeline01 ended with failure=null, all steps exit0/no timeout. Both matrices
have nine audited arms; zero-control parent/candidate outputs are exactly equal
at all three rates. Full archives are SHA-verified on P and locally:
7cd5ea6aa2d7be05ca2a76ee81b04b05696e42aacf7b12e8588b874011ff6ff0 (equality),
e85cb41ad8a72fdd9ef57235a8978197f7a9909183f545726d44057a89eefa5e (charged sharing1).

Charged sharing1 parent -> dirty-scan p99 seconds, in ascending rate order:
16.54118 -> 14.17194; 15.93506 -> 18.03983; 16.07507 -> 20.53416.
Only low wins. Do not promote, use a different round's favorable reference, or
call sharing1 measured hardware. Full metrics including regressions are in
`evidence/forecast-feedback-20261027/dirty-scan-charged-summary-01.json`.

### What was observed

The original controller CPU model is not simply absent. Offline reconstruction
uses only completed labels at the original origin (boundary quote is reconstructed,
not originally logged). Delta02 middle route MAE .2733s vs completed-mean .3840s;
boundary .2929 vs .3309. For its worst headless-terminal:12, route CPU predicted
1.10158s vs actual1.05157s; boundary predicted .67913 vs actual .10627s.
Its extra tail cannot be explained as missing controller cost alone.

At that request's route time200.5065s, four D0 holders were already known. Later
routed dna-insert:13 (203.0999s) finished P first (211.3494s), taking D0's released
slot at214.5099s. Target P ended214.3191s, but D control started217.6208s. D1 was
empty at P completion. This is an observed3.30167s slot wait, NOT a recoverable
gain estimate: alternate H2D/copy/compute/control and future feedback still matter.

Across delta02 middle candidate,22 requests waited for D admission and7 saw an
empty alternative at P completion. In dirty-scan middle,23 waited/5 had an empty
alternative; in high,31/6. However dirty-scan middle/high worst requests had zero
D admission wait themselves. The mechanism is real in the CPU execution model
but is NOT a complete explanation for all tails or hardware congestion.
`analyze_route_staleness.py` retains all95 original route origins, actual P ends,
D holders/waiters and later predecessors. Observing them offline does not grant
future-arrival knowledge to a policy. `D-binding-mechanism-01.json` indexes the
full local alignments and retains representative raw rows and hashes.

### Evidence-directed candidate and explicit costs

`late-decode-reconsider-01` retains release-preserving-cobatch01, not the losing
micro-optimization. A tentative D can be reconsidered only after actual P
publication, before any D acquisition, and only if its observed queue/slots
block admission while the alternative currently has capacity. Two D destinations
and published-prefix/recompute choices share the retained64-candidate budget.
The same cache-evolving resource calendar prices H2D, complete decode/save,
source/destination gates, copy/link and control. No balancing quota or fitted
benefit threshold. Existing pending-first latency profiles break ties consistently.

The search/RPC interval holds no device gate or sequence slot. Its measured wall
cost delays actual D enqueue; an additional commit cost is charged. A mean of
only previously completed reselect calls predicts this new control operation;
before observations it is zero plus the retained RPC cost, an explicit initial
uncertainty. All decisions log this estimate and actual cost. The owned D suffix
alone changes; P and other jobs are untouched. Actual admission still rechecks
cache/boundary. Future arrivals may invalidate the new quote too; no guaranteed
hit, reservation or monotone improvement is claimed.

The shared default enqueue hook is mechanically unchanged; the calendar now
supports explicit future release of an unacquired D suffix, without reserving
its slot in advance.14 focused checks cover no early slot ownership, other FIFO
progress, acquired/unpublished rejection, two-destination budget, guards and
co-batch behavior. Ruff/scoped diff checks passed. No Rust/NPU serving changes.
Frozen154-file archive SHA578834dcd8a35c6f57b886526d659167e43430d5aa257f97d556edcf03043c98.

Next run: one bounded pipeline, default-sharing0 three rates then sharing1
sensitivity three rates, each with native/reference/candidate and full charged
cost. No GPU performance claim. Inspect actual launch/result receipts, archive
and audit before concluding. Independent late-D audit checks original/final
route ownership, completed-only estimates and decision cost before D admission.
Hourly research remains ACTIVE. Retained default is still TimedMemo-normal-910B2.

Actual next launch verified: supervisor101619 at Unix1793052696.6757019,
controller101621, workers101622/101623/101624. Limit4500s overall,1800s per matrix,
120s per audit; three workers on4CPU/BLAS1. Native arms in the default0 stage had
completed at initial verification, not the entire matrix. No final result then.
Authoritative `/root/issue23-late-decode-pipeline-01/result.json` when present;
log `/root/late-decode-pipeline-01.log`. Independent frozen source and outputs
must not be modified. At the next hourly turn, inspect once, finish full archive/
SHA/source/accounting/late-decision audit, align original vs final route and
critical requests. If completed without useful gains, use the measured reselect
cost and changed state paths to revise the hypothesis; no blind rerun. Do not
append additional matrices merely because this one ends. Research ACTIVE.
## Latest checkpoint: late-decode completed; progress-preserving advice launched

This section supersedes the older running statements below. The late-decode01
pipeline completed all12 steps with exit0, no timeout/failure. All18 CPU arms
passed source/input, accounting, work conservation, completed-history/release
and late-ownership audits. Full archives verified on P and locally:

- default0: aa09a15e9b6751b5dbdf7fc7772ab6a1bba19f6060f1ab69c1a50496f2b33a98
- sharing1: fe08e6ff6fa9785c98b0bea8ec1abb08097869cfcc26085e7830323fa8b8a156

Local full results are under cache-profile-20261026/result-late-decode-SETTING-01.
`evidence/forecast-feedback-20261027/late-decode-completed-summary-01.json`
contains all six paired metrics, raw hashes and worst-request alignments. The
reference is release-preserving-cobatch01 from the SAME pair, not TimedMemo or
a more favorable reference from another run. Sharing1 remains an unmeasured
sensitivity boundary. None of this is actual parallel serving evidence.

| Scenario / rate | Reference p99(s) | Candidate p99(s) | Reduction |
| --- | ---: | ---: | ---: |
| default / .2452 | 20.2886 | 20.4379 | -0.74% |
| default / .4903 | 21.1210 | 20.6854 | 2.06% |
| default / .9806 | 19.9646 | 18.5751 | 6.96% |
| sharing1 / .2452 | 18.9794 | 21.3485 | -12.48% |
| sharing1 / .4903 | 21.6468 | 17.4883 | 19.21% |
| sharing1 / .9806 | 15.4444 | 15.7494 | -1.97% |

Do not promote this mixed candidate. Default makespan changes are small/mixed;
net TTFT improvements cannot be inferred from the selected D count alone.

Observed mechanism: default search calls50/34/40 produce only4/3/4 D changes,
costing23.56/26.20/21.48s extra measured CPU in total. Sharing1 calls10/21/32
produce0/0/1 D changes. Same-D prefix commands change0/2/1 times in default,
0/2/0 in sharing1. Thus the sharing1 middle gain is NOT evidence of successful
rerouting: no D changed, although two prefix commands did and subsequent
control/interleaving/cache histories diverged. Do not optimize by adding delay.

In sharing1,7/7/9 calls had no prior waiter and an observed original-D slot
release before the delayed decision completed. The former implementation kept
the request OUT of its original FIFO during that computation. Sum of excess
delay after those observed releases is3.55/3.15/3.69s, NOT counterfactual net
benefit: the observed trajectory already includes the policy's effects. Default
counts are2/1/0, so this mechanism cannot explain all losses. In sharing1 low,
the candidate's worst feal:11 changes TTFT11.9004→21.3485s against the same
request: P-to-D rises.5792→13.3464s, while P API drops9.3116→5.8808s. The new
global maximum is a changed path, not an additive network-congestion estimate.

### Next candidate: progress-preserving-decode-01

Preserve release-cobatch01, immutable P/acquired work and the existing input,
seed, capacities, costs, candidate budget and three rates. At actual P
publication the request enters its original FIFO immediately. Optional advice
runs only while that request is still unacquired, observed slots/older waiters
prevent its admission, and the alternate D currently has room and no queue.
No forced balancing, task/rate branch or fitted benefit threshold.

The incumbent is now an explicit immediate no-op forecast; alternate-D prefix
choices include estimated reselect CPU plus retained RPC before their enqueue.
Both use the same full cache-evolving resource calendar. A move needs strict
improvement of the retained pending-first profile. One no-op plus at most63
alternate choices share the original64 budget; actual D admission still searches
and validates its boundary. Only previously completed advice calls estimate CPU.

Original D execution proceeds while advice is pending. To avoid an uncharged
parallel-control assumption, advice and initial routing share ONE serialized
CPU lane; new requests can wait behind its actual measured CPU/RPC. Already
eligible new routes get priority before optional advice starts. D boundary
control retains the parent's separate execution model, not newly validated
hardware. Advice has no sequence or device lease. If original D acquires first,
discard the advice. If alternate admission is no longer available, discard it.
Otherwise change only the still-owned queued suffix; preserve all other FIFO
order and P state. Actual search/RPC/commit and final drain remain charged,
including discarded advice; no double-adding overlapped wall time to TTFT.

14 focused tests and Ruff/scoped diff checks passed: immediate-vs-delayed no-op,
bounded search, acquired/completed cancellation, no queue destruction, owned
commit, CPU release/cost and existing late/publication guards. No native rebuild,
model replay, workload change or live service modification.

Frozen158-file source archive progress-decode-source-01.tar.gz SHA
6d6d2f2d86e9ff64555b5bd13bb0bcf4e49f964a8130f08235ebc7905d1971da.
Actual supervisor141990 started Unix1793055719.5486372. Pipeline
/root/issue23-progress-decode-pipeline-01; script /root/progress-decode-pipeline-01.py;
log /root/progress-decode-pipeline-01.log. Bounded4500s total/1800s per matrix,
three workers on4CPU, BLAS1. Default0 then sharing1, each paired with native and
release-cobatch. Independent audit_progress_decode.py checks serial control,
FIFO preservation, cancellation, original/final route and completed-only costs.
Launch does not assert completion or gains.

Next hourly continuation: inspect actual process/result/log once; do not restart
or change frozen sources. Complete archives/SHA and all audits before evaluating.
Then align cancelled/moved/unchanged advice, CPU lane delay, cache acceptance and
the original worst requests. If advice remains expensive and mostly rejected,
use that evidence to investigate decision-work bounds, not a guessed gain quota.
Research remains ACTIVE hourly. Retained default remains TimedMemo-normal-910B2.

## Progress01 completed; yielding advice running

This supersedes all older launch/pending descriptions in this document.
Progress01 ended exit0 with all12 pipeline steps successful/no timeout. Both
three-rate matrices contain9 arms; all18 pass independent source/input,
work/accounting, completed-history/release and owned-advice/FIFO checks.
Original P/root plus local full archives have matching SHA:

- default0: cd7ab310041353d438bf32cf94b301cc8d96eeb7eefdb8ccfad2298f446f0f00
- sharing1: 79df36a33f87a9604c1f887fce4beeae1c4fa81740863a49a0024626542c8c29

Local result-progress-decode-SETTING-01 contains the complete extracted record.
Compact full-metric/critical-request summaries and full diagnostic SHA indexes
are in evidence/forecast-feedback-20261027/progress-decode-completed-summary-01.json
and progress-decode-restore-summary-01.json. These are CPU simulations; sharing1
is an unmeasured boundary, not hardware. Source/work audits are not real-serving
performance certification. Retain candidate; do not promote it.

| Scenario/rate | Paired release-cobatch p99(s) | Progress01 p99(s) | Reduction |
| --- | ---: | ---: | ---: |
| default/.2452 | 18.4026 | 17.0785 | 7.19% |
| default/.4903 | 17.9411 | 15.6768 | 12.62% |
| default/.9806 | 18.1572 | 20.3611 | -12.14% |
| sharing1/.2452 | 15.6262 | 18.2632 | -16.88% |
| sharing1/.4903 | 14.5857 | 17.6836 | -21.24% |
| sharing1/.9806 | 15.8438 | 18.9455 | -19.58% |

### Observations and their limits

Default optional calls38/41/35 led to1/2/3 actual moves, spending10.46/7.87/9.95s
CPU. Sharing1 calls5/18/11 led to0/0/1 moves, spending2.24/7.78/4.62s CPU.
Original FIFO progress is preserved, but cancellation still occurs only after
the entire search computes. Sharing1 has2/8/2 acquired-or-completed cancellations;
observed control-lane occupation AFTER D acquisition sums1.06/5.12/1.29s.
It overlaps ready-to-route waits for1/9/3 later requests, totaling.865/5.132/2.070
request-seconds respectively. These overlap sums are trace accounting, not net
wall/p99 gains or a causal ablation. A shared time interval can affect many
requests. The old run is not rewritten by subtracting them.

Crucially, NONE of the six candidate worst requests directly overlaps optional
advice in its own ready-to-initial-route interval. Their paths differ through
earlier timing, routing, cache and resource feedback. Default high headless:12
changes from P0/D0 to P1/D1, accepted P prefix7680→640; before-P1.556→6.653s,
P API2.151→5.462s, D API-to-first.595→7.725s, while P-to-D7.015→.521s.
Do not attribute its entire regression to the cancellation bottleneck.

Sharing1 high headless:11 IS the single moved request. At P completion179.052s,
the current D0 has4 holders, D1 is empty. The planner's no-op predicts its own
first181.750s; move predicts185.080s, but the system pending-first profile's
maximum improves17.873→16.579s. This intentionally trades one request for the
profile; it is not a bug merely because that request gets slower. Actual first
is187.446s. D1 accepts9600 prefix tokens and its restore path is5.5053s. A
completed-history-only estimator reconstructed at D-control start, conditioned
on that observed accepted prefix, predicts3.0681s (under by2.4372s). This is
NOT the initial route's quote; after-P choice and admitted cache must stay distinct.

Across all warm D phases, signed errors vary both ways. Example sharing1 high
candidate D1 aggregate predicted20.173s vs actual21.518s over11 restores, yet
MAE1.099s and worst underprediction2.437s. Near-unbiased aggregate work does not
guarantee correct tails or correct resource ordering. No constant H2D penalty is
justified. Timers include restore/backend/synchronization, not pure DMA, and all
numbers here come from the retained synthetic phase world, not new NPU runs.
The next unresolved modeling question is service residual dispersion/ordering,
not an asserted RDMA bottleneck. Candidate control cancellation tests one narrower
mechanism while this limitation remains explicit.

### Frozen next CPU candidate: yielding-decode-advice-01

Keep progress01's no-op, initial snapshot, full resource calendar, ordering,
strict profile improvement and64-candidate ceiling. Break optional computation
at each candidate boundary. An original D acquisition/completion or an actually
eligible causal request waiting for initial routing cancels the remaining search.
No future arrivals or real future service durations determine cancellation.
One already-running calendar is not preempted internally. Discarded computation
remains charged; the no-op's queue continues. Initial route and advice remain
one serialized CPU lane. Only FULL completed searches train full-search CPU
estimates; cancelled partial work is not treated as a completed full-cost label.
Uninterrupted search is tested to produce the same candidates and result as
progress01. No thread/GPU/extra CPU capacity, task/rate exception or quota added.

Ten focused checks plus Ruff pass, covering exact uninterrupted search, stopping
the remaining evaluations, acquired/eligible-causal cancellation and the retained
ownership/no-op contracts. The independent audit also verifies every measured
quantum and single RPC, FIFO/immutable P, partial-label exclusion and serial CPU.

Frozen162-file archive yielding-decode-source-01.tar.gz SHA
b6dc255dd8886b0d3146f2532c994d7876e69d2f9c39f20d951140b460260a94.
Supervisor185017 started Unix1793058968.734034; actual pipeline start
1793058968.9290335. Controller185019/workers185020/185021/185022 were observed
running the frozen source, default stage native/reference arms complete; no final
result then. New directories /root/issue23-yielding-decode-default-01 and
/root/issue23-yielding-decode-sharing-01. Each has FOUR arms: native, paired
release-cobatch reference, paired progress01 parent, new candidate. Thus compare
both parents within this same run, never pick an older favorable reference.
Default0 then sharing1 run sequentially, three CPU workers/4core/BLAS1,
4500s overall/1800s per matrix/120s per audit. No NPU serving claim.

Authoritative result /root/issue23-yielding-decode-pipeline-01/result.json;
log /root/yielding-decode-pipeline-01.log. Do not duplicate/restart a running job.
Next hourly continuation: archive/SHA and all audits, then align partial/full
search work, cancelled lane time, eligible-route waits, actual moves, cache
acceptance, phase errors and both same-run comparisons. If cancellation merely
reduces CPU without p99 gains, retain that result; investigate phase residual
dispersion using completed-only information, not stronger guessed H2D penalties.
Keep original costs and exposed95/8 replay scope. Research ACTIVE, default
TimedMemo-normal-910B2 unchanged; no commit/push or live serving modification.


## Yielding01 failed epoch contract; independent02 running (historical checkpoint)

Latest checkpoint: 2026-10-27. Supersedes the prior running01 paragraph.
The first default matrix exited1 after235.813s, without supervisor timeout.
Native/reference/parent arms completed, but no candidate arm completed; sharing1
never started. Therefore no yielding01 performance result is valid. Exact failure
is `Observed exclusive iteration contract required`. The acquired-phase reader
captured post-setup self.now while the lazy generator passed pre-setup controller
origin to the calendar. A focused reproduction failed at1.0 versus1.25.

The minimal02 repair keeps controller origin for full CPU accounting, records
forecast_origin after setup, and uses that same epoch for acquired snapshots,
forecast and completed-history cutoff. No guard/tolerance, policy scoring,
arrival, cache capacity or cost data changed. Layout is immutable; subsequent
quanta reuse the initial layout and shadow, with fresh ownership checked on
cancellation/commit. Fourteen targeted checks pass, including the formerly
failing epoch case, uninterrupted selection, cancellation, owned commit and
acquired exclusive phase protection; Ruff passes. This is an implementation
repair, not evidence for better algorithmic performance.

Failed01 full source/log/partial-output/pipeline archive is retained on P/root
and locally, SHA d1069321aba1f83caced279e32bd38c6e2badd554bb5728f9223deb56eecfe08.
No frozen01 source/output was modified. Source02 has162 files, SHA
 e86aa21d266acce5158d4afbf252af2451e90f2527a08f5abe1d02a7934994ad.
The only four changed files are yielding search/test and its two version-aware
independent audits. Supervisor197420/PGID197420 actually launched at
Unix1793059830.8385577; pipeline1793059831.0408807. First default native arms
were observed completed, child197422 and three workers197423-197425 alive;
no final result then. Four CPU arms/rate: native, release-cobatch reference,
progress01 parent, yielding02. Three fixed rates, default0 then unmeasured
sharing1 sensitivity, sequential matrices,3workers/4core and BLAS1. Source and
settings freeze across rates; full spent CPU/RPC/commit remains charged.

Actual runner `/root/yielding-decode-pipeline-02.py`, SHA
 a0be73e3b6b680697c053b480724c3199bc3c8478f11ecc6bc59fb70a2ba9b8d;
result `/root/issue23-yielding-decode-pipeline-02/result.json`, log
`/root/yielding-decode-pipeline-02.log`; output roots
`/root/issue23-yielding-decode-default-02` and `...-sharing-02`.
4500s overall,1800s/matrix,120s/audit; no duplicate matrix or NPU work.

Next: read actual result/process; archive/SHA and inspect all source/input/work,
release and quantum/ownership audits. Compare candidate to BOTH same-run
reference and parent, preserving all wins/losses; align early cancellation,
control occupancy, cache/route changes and critical requests. A smaller CPU sum
alone is insufficient. If mixed, retain the result and diagnose residual service
ordering rather than impose a constant H2D penalty. Hourly research remains
ACTIVE, TimedMemo-normal-910B2 default unchanged, no commit/push.

## Latest: yielding02 completed; P-source revalidation01 running

This supersedes every earlier running/pending status in this file. Yielding02
finished Unix1793060661.0612552, 830.02s after its actual pipeline start, exit0,
all12 steps successful/no timeout. Source/input/work/completed-history/owned-control
audits cover24 arms (4 arms x3 rates x2 worlds). The old work-audit stdout said9
because its printed count was hardcoded; its loop and JSON cover all12 per world.
The reporting count is corrected in the new source, not the frozen result.
The six same-round progress-parent arms also pass their own independent advice
audit. Complete archives remain on P/root and locally with matching SHA:

- default0: ce1bef6c3b3fe6ac69deb1a73d33dbcd1c84c5b20d69c4d0b33b16afd5d78b16
- sharing1: 7876fb17b6872baa3b82f568006a017bf6ab3a3c1842a9d9cb4d6510ce658620

Full-metric/work/drain/control/worst-request summary is
`evidence/forecast-feedback-20261027/yielding-decode-completed-summary-02.json`,
SHA49c2388e81900f50ff69bcc459054df02c04f09c15a413ff6fb474d145a66321.
Its hash index identifies the full380-path diagnostic reports per world in the
external cache-profile directory. Every original full arm and failure is retained.

| World/rate | Release-cobatch p99 | Progress01 p99 | Yielding02 p99 | Gain vs release / progress |
| --- | ---: | ---: | ---: | ---: |
| default/.2452 | 18.4000 | 17.9525 | 17.2182 | 6.42% / 4.09% |
| default/.4903 | 19.8197 | 18.3291 | 18.3248 | 7.54% / 0.02% |
| default/.9806 | 25.6985 | 20.2508 | 18.2514 | 28.98% / 9.87% |
| sharing1/.2452 | 15.9760 | 17.0468 | 19.3089 | -20.86% / -13.27% |
| sharing1/.4903 | 13.8641 | 15.0537 | 15.7286 | -13.45% / -4.48% |
| sharing1/.9806 | 19.6612 | 16.9044 | 16.1893 | 17.66% / 4.23% |

These are fully charged CPU causal-replay TTFT seconds, not NPU performance.
Sharing1 remains an unmeasured sensitivity boundary. Do not promote this version
or erase its losses. TimedMemo-normal-910B2 remains the retained default.
The mean/full-completion/nominal metrics are in the summary, not assumed to win.
All six candidate worst requests have zero direct optional-advice overlap before
their initial route. Sharing1 candidate advice makes ZERO actual moves in every
rate; changes still feed through CPU timing, arrival/session ordering and caches.
Smaller optional-control CPU therefore is not by itself a tail explanation.

### Observed source-gate wait, separated from wire time

`analyze_copy_waits.py` partitions copy-wait windows into disjoint source gate,
destination gate, direction slot, overlaps and unexplained/bookkeeping intervals.
Exact P dequeue was not archived: max(route completion, previous same-P completion)
is only a FIFO-ready LOWER BOUND. D API residence is never treated as exclusive
device ownership; only actual batch iteration intervals are used. These sums
are trace accounting, not hypothetical recovered wall time or p99 gains.

Sharing1 low yielding02's headless-terminal:10 is the worst request (19.3089s).
It routes at284.3081s, completes route control before its P slot is free, and
has a FIFO-ready lower bound291.251549s. Its P1 source was acquired by the later
git-multibranch:10 at290.002136s and released at297.765240s. The actual history
copy starts297.765863s and lasts only0.039137s, following6.514314s of post-FIFO
lower-bound waiting. The original target forecast preceded that later request's
admission. Original source/target gate constraints already exist; the additional
problem is a recovery decision kept after its queue/resource context changes.
This supports source-access contention, NOT measured RDMA bandwidth congestion.

Candidate sharing1 low has10.7875s summed copy-wait exposure, almost all source
gate, versus6.5453s total copy execution; these aggregate times are not a latency
denominator. Middle has only0.3199s exposure, so this mechanism cannot explain
every loss. Default exposures are1.2721/1.1355/.5839s. The full per-request split
is retained in yielding-decode-{default,sharing}-copywaits-02.json.

`analyze_prefill_revalidation.py` reconstructs ONLY seed plus completed P/copy
observations at each lower-bound checkpoint. Live P age is right-censoring; it
uses completed issued/actual ratios, never the live source's future end. It does
not replace an initial command with later acceptance. Missing exact dequeue,
inventory feasibility and new control/global-tail effects remain explicit.
At the critical checkpoint the issued source point is7.7391s, age1.2494s, and
completed-only residual predicts5.9931s. Target warm P is4.1612s vs cold4.6203s.
Even a zero-cost copy would leave a5.534s predicted physical advantage for cold
P. A conservative full-prefix copy quote produces5.691s; actual missing bytes
are smaller, so that upper payload quote is NOT used as the deployed decision.
This is a mechanism screen, not a policy-feedback p99 improvement. Only two
sharing-low candidate requests show positive cold margins within its narrow
active-P-source scope; do not generalize it to all requests or all rates.

### Frozen candidate prefill-source-revalidation-01

Parent/reference: yielding-decode-advice-02. Trigger only when an actual P FIFO
head is unacquired, route CPU is free, an available remote P prefix is requested,
and its source P gate is currently owned by another P API. Keep remains an
immediate comparator. Evaluate at most local-prefix and zero-prefix alternatives
with the full admitted-work pending-first profile and cache/resource calendar.
Alternatives include completed-only estimated controller CPU plus RPC. No forced
action, quota, penalty, task/rate rule, future source end or physics label.

Actual decision shares the initial-route/D-advice CPU lane. The P FIFO head waits
for this decision, preserving its order while holding NO device or sequence
lease; D/other device progress continues. Its measured CPU/RPC/commit and any
downstream waits remain charged even when the decision keeps the old route.
Forecasting adds the same control prefix to that FIFO position, so younger P
work cannot incorrectly overtake it. Commit verifies owned unacquired head;
actual stage admission rechecks current prefix/capacity. Already acquired work
cannot be rewritten. Unknown future revalidation decisions are not oracles in
an earlier initial forecast; closed-loop prediction error must be measured.

17 focused checks pass locally AND on P: active-work refusal, immediate no-op,
charged control changing a choice, preserved FIFO in the real calendar, and the
existing epoch/cancellation/past-only contracts. New source169 files,
`prefill-revalidation-source-01.tar.gz` SHA
a6b4a3ecbf6c4c1f0f994f43a392b6dd4373db1bdf0af9ad4cc6b4f9e2e4f8c5.
Runner SHAe3e9bf06950a2be8453ec891f7e69e8cf2dcc50084b65082b427c9fcd052bb33.
SupervisorPID/PGID249825 launched Unix1793063769.1426573, actual pipeline start
1793063769.314359. Default native arms completed; controller249828 and workers
249829/249830/249831 observed alive. No final result at that checkpoint.

Actual runner `/root/prefill-revalidation-pipeline-01.py`; authoritative result
`/root/issue23-prefill-revalidation-pipeline-01/result.json`; log
`/root/prefill-revalidation-pipeline-01.log`; outputs
`/root/issue23-prefill-revalidation-{default,sharing}-01`.
Default0 then sharing1, three rates, native/reference/ours,3workers/4core/BLAS1,
4500s overall/1800s per matrix/120s per audit. Only CPU jobs; no new native build,
real GPU/NPU result or parallel serving validation. Do not edit frozen source.

Next continuation: inspect actual result and complete archives/SHA. The new
audit_prefill_revalidation.py checks live-source trigger, no earlier acquisition,
P FIFO and full shared CPU serialization, past-only estimates, strict profile
improvement for changes, real no-copy outcomes and D advice ownership. Then align
P changes, source waits, recomputation, global resource/cache feedback and net
p99 against SAME-ROUND yielding02. Retain losses and zero-action cases. If this
fixes source wait but loses overall, diagnose which downstream release/cache
path absorbed it instead of escalating a restore penalty. Hourly research ACTIVE;
no commit/push, no replacement of the retained default.

## Final checkpoint this continuation: P-revalidation01 completed, not promoted

Supersedes the running state above. Pipeline finished Unix1793064350.2409787,
580.927s elapsed; all13 steps exit0, no timeout/failure, no remaining controller
or workers.18 CPU arms (native/yielding02/new x3 rates x2 worlds) pass source,
input, complete-work/accounting, release estimate and independent P/D control
ownership/history audits. Complete archives have matching P/root and local SHA:

- default0: 6751780fb4682f4fbec4fb400b60160a38c92aaf0fb133a5ed7e56202717cd9b
- sharing1: 6d76d703a4028c7e67416cb22540f71e54b85897eb2445ce32edc6bb3aeb15f6

`prefill-revalidation-{default,sharing}-feedback-01.json` retains full TTFT,
nominal, final-drain makespan, CPU components, aligned action/worst requests,
copy-wait partitions and first original-route divergence. Result/archives and
reports are indexed in the version registry/evidence directory. The raw input,
calibration, physical capacity and model remain unchanged. These are not NPU runs.

| World/rate | Same-run yielding02 p99(s) | New p99(s) | Reduction | P decisions/changes |
| --- | ---: | ---: | ---: | ---: |
| default/.2452 | 18.8607 | 18.6872 | 0.92% | 0/0 |
| default/.4903 | 18.3627 | 22.0471 | -20.06% | 1/1 |
| default/.9806 | 20.3469 | 25.3493 | -24.59% | 0/0 |
| sharing1/.2452 | 18.4194 | 21.5221 | -16.84% | 0/0 |
| sharing1/.4903 | 18.0136 | 17.7093 | 1.69% | 1/0 |
| sharing1/.9806 | 17.0714 | 21.3133 | -24.85% | 0/0 |

Do not call these gains/losses pure effects of P revalidation: FOUR candidate
arms did not invoke it. Their inherited initial routing/advice have measured CPU
timing differences and diverge before any new action. Default-low first initial
route difference occurs at fix-git:2, default-middle at headless-terminal:3,
default-high at headless-terminal:4. The middle's only P change happens much
later, at feal:7. The no-call arms are evidence that one paired CPU replay does
not isolate a small optional policy's effect. This is also not unexplained RDMA
contention: world/data are fixed, but measured control costs enter its causal
clock and can change dispatch, cache and session trajectories.

### Verified numerical defect and next hypothesis

Middle-default's sole change sees keep primary6.344471176237789s and local
6.344471176237761s: one absolute-timeline ULP,2.842170943040401e-14s apart. Its
next pending latency worsens5.8680→6.0873s and the target first prediction
169.9149→171.6830s. The raw lexicographic test accepts the tiny primary change.
This is not credible benefit evidence; no arbitrary percentage/millisecond
threshold is introduced to hide it.

`diagnose_calendar_roundoff.py` now independently reproduces the mechanism:
two streams have NO shared gates/cache/link/batch. Change only the unrelated P
duration. Coalescing changes a's first endpoint between adjacent binary floats
173.24447117623777 and173.24447117623774. Disabling coalescing leaves exactly ONE
binary endpoint for all9 unrelated durations. Different decode grouping and
floating accumulation alone can therefore change a strict profile comparison.
This proves a numerical mechanism in the calendar, not every large tail delta's
cause or a corrected policy's benefit. Reproduction SHA
d011938c596c2b8fc0b3a5a145796be912211339790ffe27c83aaeb7913ff12a.

The busy-source problem is still an observed bottleneck, but this candidate
rarely encounters the earlier path under changed timings and does not establish
robust improvement. Do NOT expand triggers or add restore penalties to force
actions. Retain all results, prior promising versions and TimedMemo reference.

Next hourly work is specific: first make coalesced forecast comparisons
numerically reproducible against an uncoalesced/exact-time mechanism reference.
Prefer a proved arithmetic/provenance or error-bound method over a fitted gain
epsilon. Preserve all work/resource/capacity rules and measure its CPU overhead.
Then distinguish equal-policy control-timing feedback from policy decisions
with fixed-state/equal-control diagnostic runs (clearly not net performance),
before another fully charged matrix. Global no-op and sorted tail objective
must not choose a worse secondary profile solely because of rounding. No new
candidate is currently frozen/launched; numerical repair remains pending.
Research remains ACTIVE hourly, no commit/push, no NPU deployment or promotion.

Subsequent numerical repair and its new diagnostic/charged checkpoints are in
`numerical-calendar-20261027.md`. Its frozen source keeps the prior versions and
changes only forecast time arithmetic; consult that file and the registry for
current activity rather than relaunching any completed pipeline above.
