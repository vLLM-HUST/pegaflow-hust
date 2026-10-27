# Shared policy research implementation

Current snapshot: [status, known limitations and next work](../../docs/research/README.md).
The opt-in staged-save model is incomplete; read its limitations before running it.

Start with [the research handoff](../issue23_handoff/README.md), including metric,
baseline and evidence limits. This directory is an isolated research overlay;
it does not replace the installed serving connector or native engine.

- `simulate_policy_feedback.py`: three-rate causal-feedback CPU screening.
- `simulate_continuous_batch.py`: bounded iteration-level D batching and a
  shared iteration kernel; [assumptions and reproduction](CONTINUOUS-BATCHING.md).
  This is a CPU candidate, not an installed parallel vLLM worker.
- `memo_search.py`, `causal_prefill_search.py`, `fair_endpoint_search.py`:
  [PCIe/endpoint iteration and limitations](PCIE-ITERATION.md). The fair variant
  removes a legacy world/forecast resource-arbitration mismatch; do not treat
  old node-order results as an independently verified native-server behavior.
- [PCIe research/model update](../../docs/research/tailguard-pcie.md): Strata,
  TurboBus and recent papers, with primary sources and hardware boundaries.
- [910B2 memory update](../../docs/research/910b2-memory-20261026.md): removes
  the old 2 GiB device KV default, with hardware/weight provenance, explicit
  runtime-headroom sensitivity and new auto-profile serving source staging.
- [Executable paper baselines](../../docs/research/paper-baselines-20261026.md):
  scoped Mooncake, MemServe and Strata adapters in the same three-rate CPU
  matrix, including explicit missing mechanisms and hardware assumptions.
  Historical `FairMemoSimulation` is our internal FIFO/memo reference name,
  not an external paper reproduction.
- [Bounded forecast search and retained versions](../../docs/research/bounded-search-20261026.md):
  strict event-time lower bounds and decision-local rejection proofs reduce
  calculation without a new performance weight. Equal-cost mechanism replay
  and fully charged causal performance are reported separately.
- [Cache-evolving batch forecasts](../../docs/research/cache-evolution-20261026.md):
  an evidence-led candidate couples publication, reclaim and owned leases to
  the batch calendar. Two complete three-rate CPU comparisons are retained;
  reverse-order low-rate p99 regressed, so this is not the default policy.
- [Sparse protection updates](../../docs/research/sparse-cache-20261026.md):
  profile-led CPU reduction with exact mechanism replay; a large high-rate
  tail regression is retained rather than hidden by lower average overhead.
- [Controller release calendar](../../docs/research/control-release-20261026.md):
  models the route/D-control completion boundary before resource acquisition,
  using completed same-arm CPU observations. This is an experimental candidate.
- [Visible-work CPU model](../../docs/research/control-work-20261026.md):
  predicts control release from observable work with completed-only NNLS. Three
  three-rate comparisons and component diagnostics retain the high-rate failures;
  lower prediction error does not automatically mean lower closed-loop p99.
- [Joint phase-residual model](../../docs/research/phase-residual-20261026.md):
  preserves aggregate service while locating residuals on exclusive restore/save
  resources. Includes a fixed-route mechanism diagnostic and the admission-order
  feedback of the round-robin native proxy. This is not hardware validation.
- [Completed exclusive D-phase feedback](../../docs/research/completed-phase-20261026.md):
  trains restore/finish components from completed owned intervals, excluding
  batched residence and charging maintenance. The first three-rate win failed
  reverse-order confirmation; both results and request-level explanations remain.
- `runtime/code/shared_pair.py`: six fresh arms, B/T at rates 300, 150, 600.
- [Acquired exclusive phases](../../docs/research/acquired-phase-20261026.md):
  retain active H2D/save ownership and condition remaining time on observed age;
  a completed three-rate screen had mixed p99, so this is not promoted.
- [Pending-first compute](../../docs/research/pending-first-20261026.md):
  first-token priority with full decode and resource release conserved. Both
  parent arm orders and a retained-TimedMemo comparison improved all three CPU
  TTFT tails, but full completion/nominal regressions remain. Preserved candidate,
  not real parallel-serving validation; all results and reproduction are linked.
- `runtime/code/audit_shared.py`: candidate-specific result/source/receipt audit.
- [Known-slot release and shared-work sensitivity](../../docs/research/slot-release-20261027.md):
  follows the observed D-slot bottleneck with a completed-only release rule;
  preserves mixed full-completion outcomes and a sharing sensitivity failure.
  The follow-up retains predicted non-extending decode companions. CPU evidence
  remains separate from real continuous-batching qualification.
- `runtime/overlay/pegaflow/`: immutable live-parent dependencies.
- `runtime/calibration-seed/`: original 32 isolated requests / 64 stages.
- `runtime/workload/`: frozen replay and source-selection manifest.
- `../../python/pegaflow/strategies/`: common native, pending-tail and explicitly
  named MemServe/Mooncake strategy adapters. The latter are not full systems.

The snapshot's `source.sha256` and `confirmation-sources.json` apply to the
original 2026-10-16 runtime files only. The portable simulator entry point was
extended for this handoff, outside that immutable runtime. No completed new
native-baseline versus pending-tail real A/B was verified at handoff.

For a fresh clone's CPU run, first retrieve the external collection according
to `../../EVIDENCE.md` and set `BENCHMARK_COLLECTION_ROOT`:

```bash
PYTHONPATH=python python experiments/issue23_strategies/simulate_policy_feedback.py \
  --input-bundle "$BENCHMARK_COLLECTION_ROOT/experiments/issue23_handoff/simulation-inputs.json" \
  --seed "$BENCHMARK_COLLECTION_ROOT/experiments/issue23_strategies/runtime/calibration-seed" \
  --output /tmp/tailguard-screen-unique.json
```

Live launchers retain historical `/root` paths and endpoints. They are not
portable autostart scripts. Inspect ownership, output absence, allocation,
runtime/native hashes and current addresses first, stage a new immutable job,
then use a bounded supervisor. Never overwrite or blindly rerun an old job.

Latest CPU research continuation: `../../docs/research/forecast-feedback-20261027.md`.
Dirty-scan01 completed with low-rate-only sharing1 benefit and middle/high losses;
not promoted. Late-decode-reconsider01 now tests destination staleness at actual
P publication under measured reselect/RPC cost, without slot ownership while
planning. Frozen source/launch and all previous negative outcomes are retained.
Hourly iteration remains active. Real parallel serving is still unvalidated.
Latest CPU continuation (2026-10-27): see
[`forecast-feedback-20261027.md`](../../docs/research/forecast-feedback-20261027.md).
Late-decode01 completed18 audited arms but mixed gains; not promoted. The next
`progress-decode` candidate keeps original D FIFO progress, prices an immediate
no-op against delayed alternate advice, cancels acquired work and charges advice
on the serialized routing CPU lane. Frozen default0 and sharing1 three-rate
pipeline is launched; inspect its actual receipt/result before claiming gains.
Real parallel serving remains unvalidated; TimedMemo-normal-910B2 stays the default.

2026-10-27 newest continuation: progress01 completed all18 audited CPU arms with
mixed results; yielding-decode-advice01 now runs both default0 and sharing1 at
the same three rates. It includes paired progress01 AND release-cobatch controls.
Read the final checkpoint in docs/research/forecast-feedback-20261027.md before
older status paragraphs. Default retained reference and real-serving limitations
are unchanged. No old or active frozen output/source is overwritten.


Newest checkpoint: yielding01 stopped on a reproduced setup/snapshot epoch
mismatch before any candidate result. Full failure retained. Independent
`yielding-decode-advice-02` repairs that contract;14 focused checks pass and
four-arm/rate default0 then sharing1 CPU matrices are running. Read the final
forecast-feedback checkpoint and actual pipeline result. No performance claim
yet; no real parallel deployment, no promotion of the default reference.

Newest checkpoint supersedes the above running state: yielding02 completed24
audited CPU arms, with default three-rate wins but sharing1 low/middle losses.
See the latest section of `docs/research/forecast-feedback-20261027.md` for all
same-round comparisons and original archive SHA. The new frozen
`prefill-revalidation` pipeline now tests stale P recovery boundaries at an
unacquired FIFO head with a busy remote P source. It compares keep/local/zero
using the global pending-first forecast, preserves FIFO, and charges new control
on the existing serialized CPU lane.17 targeted checks pass locally and on P.
Use `/root/issue23-prefill-revalidation-pipeline-01/result.json` for real status;
do not duplicate running matrices. Results pending; retained default unchanged,
hourly research ACTIVE, real parallel NPU serving still unvalidated.

Newest result: prefill-revalidation01 finished18 audited CPU arms; no active job.
It is NOT promoted: four of six candidate arms had no P-revalidation call,
while measured-control timing still changed later routes/caches. A separate
independent-stream fixture reproduced a coalescing roundoff defect relevant to
the sole accepted P change. Next work is reproducible calendar arithmetic and
control-feedback isolation, not wider triggers or a gain threshold. Read the
FINAL checkpoint in forecast-feedback-20261027.md. Both complete result archives
are verified on P/root and locally; all failures/losses remain retained. Hourly
research continues; do not relaunch this completed candidate.

Next continuation: `docs/research/numerical-calendar-20261027.md` documents
`exact-binary-calendar-01`. It uses exact sums of binary64 primitive durations
in forecasting, with one public endpoint rounding and no fitted gain epsilon.
Fixed-control replay is a separate mechanism diagnostic, never a net TTFT claim;
the fully charged replay must pay actual additional forecast CPU. Read the new
checkpoint/version registry for active state. Existing frozen sources are intact.

Newest checkpoint: source01 failed numerical-option activation and is retained;
corrected source02 passed the original-input activation check and18 diagnostic
arms, then started `/root/issue23-exact-calendar-charged-pipeline-02`. It now pays
all measured control costs. Consult the FINAL section of
`docs/research/numerical-calendar-20261027.md`, not the old diagnostic-running
paragraphs. Never report fixed-control diagnostic changes as net performance.

Newest checkpoint: corrected exact-calendar02 fully charged18 CPU arms also
finished and all complete archives/audits are verified. Default same-round p99
reductions8.12/8.40/−1.59%; sharing1 boundary17.32/−6.13/−0.93%. Not promoted;
TimedMemo remains retained. Read numerical-calendar's final section for the
P-queue regression whose original chosen forecast was already accurate, the
control/search-work increase, and the next fixed-state profiling hypothesis.
No active matrix; hourly research remains ACTIVE. No real NPU claim.

Newer checkpoint: `docs/research/numerical-choice-20261027.md` now records the
completed fixed-state profiling diagnosis. `linear-reclaim-20261027.md` is the
current entry: unchanged forecast semantics, less repeated owner-list work;
diagnostic pipeline started at `/root/issue23-linear-reclaim-diagnostic-pipeline-01`.
It must pass full three-rate replay equivalence in both sharing scenarios before
fully charged runs. Source/physics/workload are frozen. Do not interpret the
diagnostic as net p99 improvement or rerun an existing output directory.

Current continuation: linear01 now finished both diagnostic and fully charged
matrices. Default net p99 reductions +5.39/−2.25/+0.52%, sharing1 sensitivity
−37.88/+12.46/−28.30%: not promoted. Read its FINAL request/cache/queue analysis.
`docs/research/prefix-reclaim-20261027.md` records the evidence-led correction:
use the existing eligible-prefix invariant instead of Counter bookkeeping.
Source02 is immutable; launcher02 failed before samples, launcher03 repairs only
the Python path and runs new diagnostic03→gated charged03 in the background.
Inspect `/root/issue23-prefix-reclaim-chain-03` before any new launch. Hourly
research ACTIVE; all bad results/archives retained, default reference unchanged.

Latest checkpoint: prefix-reclaim-20261027.md FINAL has full-cost mixed outcomes;
no-decode-advice-20261027.md and pcie-versions active_experiment now track the
new same-parent optional advisory ablation. Do not restart completed prefix03.

Latest continuation (2026-10-27): no-decode-advice01 full18 arms now archived /
audited, default net p99 +11.60/+17.25/-9.49%, sharing1 +12.28/-7.64/-5.20%.
Not promoted. See its FINAL chronology: some initial route divergence predates
any advice; zero committed moves does not remove controller/cache feedback.
Current candidate is advice-release-window-01, docs/research/advice-window-20261027.md.
Only launcher04 /root/issue23-advice-window-charged-pipeline-04 runs the intended
policy. Earlier launcher02 ran the WRONG old policy; retained but invalid for
candidate claims. No change to retained default, input, rates, costs or budgets.
Read registry active_experiment for actual launch and next bounded check.


2026-10-27 current: `docs/research/advice-calendar-20261027.md`. Previous
release-window01 finished18 full-cost arms, mixed;7skips/151checks, retained
without promotion. Known source-device copy gate work delayed a predicted
release; new candidate reuses full incumbent DAG and prices remaining search
only after keep/RPC paid. Fifteen quick tests passed local/P; source frozen,
three-rate default and sharing sensitivity now running. See registry for actual
PID/commands, full results and next hypothesis. Hourly research ACTIVE.

- [Ready prefill / restore arbitration](../../docs/research/ready-prefill-20261027.md): current frozen full-cost CPU candidate and live receipt. Prior calendar-window18arms complete: default mixed, sharing all worse with0 cancellations; all preserved. Only older ready prefill may precede later restore; save/nonpreemption/full release costs unchanged. Actual parallel serving remains unvalidated.

- Latest: [save publication lifetime](../../docs/research/save-publication-20261027.md). Ready-prefill18arms complete/mixed; no running matrix. Minimal CPU counterexample finds simulator eviction of unpublished save allocations unlike current native source. Next verify failure/ownership and implement same correction in world+forecast before new matched full-cost simulations. Hourly research ACTIVE; all old results retained.
