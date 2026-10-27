# Linear forecast reclamation: evidence-led controller optimization

Parent: exact-binary-calendar-02; retained research default remains
TimedMemo-normal-910B2. This candidate is CPU-only and not promoted.

## Hypothesis and implementation

The same-state numerical-choice05 profile measured929 repeated list.remove
calls taking about5.14ms in one31ms calendar; arithmetic conversion was about
1.4%. A route search repeatedly forecasts publication and native batch reclaim.
Removing V owners from U entries with repeated equality scans/shifts costs up
to O(UV). Count equal victims then make one ordered pass: expected O(U+V)
dictionary operations, plus unchanged owner hashing/key accounting. Earliest
equal occurrences are removed just as in list.remove, preserving duplicates,
aliases, surviving order, protected/virtual owners and shared physical charges.

No victim choice, native512 reclaim geometry, budget, capacity, objective,
route/revalidation rule, service estimate or physical event is changed. Both
arms keep exact dyadic forecast arithmetic. All real measured controller work
continues to be charged through the existing control resource. Lower control
cost can alter later causal ingress/cache evolution, so lower p99 is a testable
hypothesis, not an automatic consequence of a faster local computation.

The first proposed subsequence helper failed an equal-object/alias test because
ordered subsequences do not generally preserve first-equal deletion identities.
It was replaced before freezing by counted-victim removal. A malformed test
demand was also corrected to retain its required logits token. Two focused
checks now pass (100 seeded owner-layout cases plus equal/alias cases); Ruff
passes scoped changed tools. These development failures are documented rather
than weakening the equivalence requirement.

## Frozen experiment

Source archive `linear-reclaim-source-01.tar.gz` SHA
`ff32196d32766d5f52712432effc99abbf0938f5b31c5911aba70326999429dc`.
Immutable P source `/root/issue23-linear-reclaim-source-01`.
Runner `/root/linear-reclaim-pipeline-01.py diagnostic` launched with stable
Python, PID/PGID358411, started1793071515.4467897. Exact command and process are
in `/root/issue23-linear-reclaim-diagnostic-launch-01/process.json`.
Pipeline limit4500s, each matrix1800s, at most3 workers, OMP/BLAS1 on4-core P.
No other matrix was active at launch. Original95/8 inputs, original64-stage
seed, three fixed rates,910B2 memory and all physical work remain unchanged.

First run default sharing0 then unmeasured sharing1 sensitivity, each with
native/reference/candidate. Measured controller time is fixed0 **only for this
equivalence diagnostic**. Require identical complete per-request/stage/resource
and batch traces, decisions and original forecasts, ignoring only method-name
metadata. Actual selected forecast hook identity AND positive removal-call
counts are independently audited. This is not a net performance result.

Only after both complete diagnostic matrices and source/work/accounting audits
pass may the same frozen source run `charged`, with actual measured controller
work, same-round exact02 reference, no parameter changes. Preserve all p50/p95/
p99/mean/completion/nominal latency and request regressions, no favorable-arm
selection. Use actual pipeline result.json before launching anything; no
duplicate jobs or edits to frozen code. Charged run not yet started here.

A separate one-shot continuation now waits for diagnostic process358411 to exit
using pidfd (no polling), with4500s wait bound. `/root/linear-reclaim-followthrough-01.py`
PID362085, started1793071765.7123888, verifies17 successful supervised steps,
both complete archive SHAs, six exact replay gates and actual activation audits.
Only then may it invoke the same frozen runner in charged mode with its own
4500s bound. Its start, charged-process, result and console are in
`/root/issue23-linear-reclaim-followthrough-01`; read them before any manual launch.
Any failed diagnostic/audit ends this continuation without a charged run.

Next interpretation: if trace equivalence passes but charged p99 remains mixed,
separate actual CPU reduction from changed calendar counts, causal ingress,
owner/cache evolution and queue placement. Do not reclassify earlier P queue
wait as unmeasured RDMA congestion. No NPU validation or generalization claim.

Progress: default sharing0 all9 arms and independent audits completed. All three
rates preserve full zero-control traces and original forecasts exactly; actual
removal hooks were verified. Full archive SHA
`d26b04c15a0c97ec4bb9b4bb39768497f1f5ed4c386ac19520a0e8e2f9c3699b`
is verified on P and locally. Sharing1 diagnostic remains running at this
checkpoint; charged continuation remains gated. This zero-change result is the
required equivalence result, not a failed performance claim.

## FINAL fully charged01 — mixed, not promoted

Both diagnostic matrices (18 arms) and charged matrices (18 arms) completed.
Charged pipeline ran581.12s,17 supervised steps exit0/no stop. Full archives and
source/input/control-accounting/work-conservation/release/forecast audits are
verified on P and locally. No NPU run is implied. The parent in every comparison
below is same-round exact-binary-calendar-02, not a selected historical baseline.

| Nominal rate | Default parent p99(s) | Linear01 p99(s) | Reduction |
|---|---:|---:|---:|
|0.2451617|19.186766|18.152578|+5.390%|
|0.4903234|17.101951|17.487072|−2.252%|
|0.9806468|22.298820|22.183894|+0.515%|

Unmeasured sharing1 sensitivity:15.511383→21.386362(−37.875%),
18.504625→16.198229(+12.464%),14.284360→18.326504(−28.298%).
Use raw JSON for full precision. These are exposed95-request nearest-rank
p99=max values, not stable/generalized effects. The default low p99 improvement
coexists with mean7.445→7.907s, p95 14.121→16.325s and makespan522.561→544.608s.
There is no all-metric win, and the small high-rate change is not robust evidence.

### Request/cache/queue explanation

- Default middle's new worst dna-insert:15 grows7.245→17.487s. Before-P grows
  0.660→9.284s; its P route changes0→1 while feal:14 occupies that P1 through
  461.824s. Accepted P prefix10112→7040 tokens increases P API5.144→6.648s;
  accepted D prefix4864→0 increases own D-to-first0.359→0.946s. Original predicted
  first470.1296 versus actual470.0277 is close. This is observed queue/cache
  evolution and route selection, not proof that selected-route queueing was omitted.
- Default high has almost identical counts/cache and the same worst fix-code:9.
  Before-P improves0.361s, P-to-D worsens0.306s, P API is exactly unchanged.
  Its tiny net change is mostly a shifted wait balance. Default middle routes
  fewer requests to P1(63→57), yet its p99 worsens: a static balancing quota is
  not justified by aggregate node counts.
- Sharing-low git-multibranch:10 grows14.729→21.386s: before-P8.168→12.462s,
  P API4.924→7.582s and accepted P prefix6784→640. Its own route computation
  grows0.627→2.297s. Original first304.176 versus actual303.891 again includes
  much of the chosen path's delay; old/new states and queues differ.
- Sharing-high feal:11 illustrates why more cache is insufficient: accepted P
  prefix6784→11776 makes P API6.919→4.698s faster, but before-P1.546→5.779s
  and P-to-D0.498→7.114s overwhelm it. D iteration history shows existing
  decode/prefill work during that gap. Do not add overlapping iteration sums
  to TTFT, relabel the entire gap pure queue, or invent RDMA congestion.
- Entire charged routing wall changes51.50→59.22s in sharing-low and70.21→85.79s
  in sharing-high. Search counts, data states, real CPU timing and subsequent
  learned control predictions all change. Fixed-state profiling supports a
  Counter-overhead concern; it does not independently attribute all aggregate
  regression to Counter. A single sequential arm order cannot isolate host drift.

The fixed-control equivalence proves the two implementations make the same
choices at identical costs/state. It does not imply fully charged causal paths
remain the same. A current admitted-only minimax objective can be correctly
implemented and still give mixed later global p99. Unknown future request/cache
paths cannot be injected to make this comparison favorable.

Charged full archives SHA:
default `1c7b9d6aae6b22d2027bfde5f8011b5338245164963dfecbffea6e4533d62cf0`;
sharing `ac2e4a166def11ab8db07fd15086309f1fc94e49fc3015633e0960a80ac123cb`.
All285 conventional measurements per world, queue context, initial predictions,
cache acceptance and complete per-request pairing are retained in the archives
and `evidence/forecast-feedback-20261027/linear-reclaim-final-summary-01.json`.
No previous failure or negative result was replaced. Continue with the narrower
eligible-prefix implementation and then charged feedback in
`prefix-reclaim-20261027.md`; hourly research ACTIVE.
