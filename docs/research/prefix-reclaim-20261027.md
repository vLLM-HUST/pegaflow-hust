# Prefix reclaim02: remove counting overhead, preserve the policy

Continue from linear-reclaim-20261027.md. Reference is the same-round
exact-binary-calendar-02. Neither linear01 nor this candidate replaces retained
TimedMemo-normal-910B2. All work here is CPU simulation, not real parallel NPU.

## Evidence and bounded hypothesis

Linear01 passed18 diagnostic arms with exact per-request/stage/resource/batch/
decision/forecast equivalence. Its fully charged default results relative to
same-round exact02 are +5.390%, -2.252%, +0.515% p99 reduction. This is mixed,
not a consistent win. Low/middle makespan and nominal latency also regress.
In middle, dna-insert:15 changes P0 to P1, accepted P prefix10112 to7040 and
D prefix4864 to0. Before-P grows0.660 to9.284s; another P1 API occupies almost
9s. Its original first prediction is within0.102s of actual. Do not call this
an omitted-queue prediction defect or RDMA congestion. Control timing causes
different admitted queues/cache paths; a current pending objective is not a
guarantee of global future p99. All alternative scores in the charged state
are unavailable, so the data do not independently prove P0 was a feasible win.

Separate fixed-state local profiling found counted-victim removal itself
0.00244–0.00287s versus legacy0.00206–0.00301s across paired order, so eliminating
repeated list.remove did not guarantee lower cost: Counter introduces hashes
and bookkeeping. A candidate using the existing eligible-prefix invariant
spent0.000217–0.000295s in that same removal function, with identical complete
calendar output. Whole-calendar timings still overlap and vary; this is one
profiled Windows state, not a remote speed guarantee or net TTFT result.
Raw helpers, source receipt, full95 replay, profiler records and exit codes are
retained in local profile-linear-reclaim-local-01 and profile-prefix-reclaim-local-02.

## Semantics and model

The allocator forecast already constructs V as the first eligible owners of U
in original order, ending when the source-derived reclaim count is reached.
Protected and virtual owners are excluded. Thus survivors are the same original
sequence with that eligible prefix removed; there is no need to hash every Unit
or repeatedly shift the list. One identity scan checks that structural invariant.
Any non-prefix caller takes the legacy general first-equal-removal function;
this fallback is a correctness precondition, not a latency-tuned threshold.
Duplicate values, shared keys, aliases, protection, capacity, events and native
batch512 remain unchanged. No model weight, new seed or balancing quota appears.

Three focused tests pass, including100 randomized owner layouts, identity/
alias equality cases and a sentinel demonstrating the actual prefix path calls
neither equality nor hashing. Keep the generic linear01 result as a failed/
partial optimization; do not overwrite its frozen source or raw evidence.

## Next evaluation

Frozen prefix-reclaim-source-02.tar.gz SHA
`c73c359bc2ee4e9a6f3772565cff47df98db4101824b9aaeacdeae9b7ed4a62a`.
Runner `/root/prefix-reclaim-pipeline-02.py` refuses to start before the previous
fully charged01 pipeline has completed successfully. Each new pipeline has a
4500s bound,3 workers maximum, original95/8 input and64-stage isolated seed,
same three rates and910B2 budgets. Default0 and unmeasured sharing1 worlds remain
separate and no result chooses a different source per rate.

Require full diagnostic equality and actual hook activation, then automatically
continue the same frozen source to fully charged matrices if all gates pass.
Archive all results and compare both TTFT distribution and the queue/cache
path of each tail request. If this still fails, do not keep changing reclaim
containers without new evidence: inspect controller-cost forecast error and
the horizon of the current pending-only route objective. Snapshot-optimal is
not future-replay-optimal; future realized work must not be supplied as oracle.

Prepared, not yet launched at this checkpoint. Consult actual process/result
and pcie-versions registry before launching. Hourly research remains ACTIVE.

## Current checkpoint (supersedes prepared above)

Linear01 fully charged results are now complete, independently audited and
archived on P/local. All six outcomes, including negative sensitivity results,
are retained in linear-reclaim's FINAL section. Prefix candidate remains source02.

First launcher02 exited before any test/simulation child: generation had changed
the stable Python path from pipeline-01 to nonexistent pipeline-02. Preserve
the failure archive SHA
`3d071a7d88b34083e64760e55aa3f2f852f38e2ba9a2f68764036aa6f8fcd485`;
there were zero benchmark samples. This was a launcher error, not policy failure.

Launcher03 repairs only that runtime path, asserts it exists, verifies/reuses
immutable source02, and uses all new03 output directories. No frozen algorithm,
physics, rate or budget changed. Runner03 SHA
`d9af4350d70a36c0d1d48107167bd852fcd2e5d921ba7487812e0b2ff95a1262`.
Chain `/root/prefix-reclaim-chain-03.py` PID/PGID381132 started1793073045.206451;
its root `/root/issue23-prefix-reclaim-chain-03` contains actual per-phase PIDs,
commands, results/logs. It executes diagnostic default0+sharing1 then charged
default0+sharing1 only after all17 supervised steps, full archive SHAs and six
equivalence/activation gates pass. Each phase4500s/matrix1800s,3workers/BLAS1.
No duplicate pipeline; read actual result before continuing. Results pending.

## FINAL fully charged outcome (2026-10-27)

Diagnostic18 arms and fully charged18 arms completed normally. Both pipelines
passed17 bounded steps, all source/input/history/full-work/activation checks.
Diagnostic full physical traces, decisions and initial accepted projections
are exactly equal to the same-round exact-calendar02 reference; method labels
alone differ. Full archives exist on P/root and locally with matching SHA.
The full-cost pipeline ran615.2278s; no NPU work or real parallel validation.

| Scenario | Rate | Reference p99(s) | Prefix02 p99(s) | Reduction |
|---|---:|---:|---:|---:|
| default0 | .2452 |19.046432|20.242410|-6.2793%|
| default0 | .4903 |20.874446|19.069986|+8.6443%|
| default0 | .9806 |20.927900|15.625966|+25.3343%|
| sharing1 sensitivity | .2452 |14.021879|15.440442|-10.1168%|
| sharing1 sensitivity | .4903 |15.903630|18.897862|-18.8274%|
| sharing1 sensitivity | .9806 |28.619534|17.455467|+39.0086%|

No promotion; retain TimedMemo-normal-910B2 as default. Gains are single exposed
95-request replay p99=max, not hardware performance, significance or robust
all-rate benefit. Reference is exact-calendar02 in this same matrix, not a
cross-run TimedMemo measurement. No wall costs were subtracted.

### Observed paths and limits of interpretation

Default low-rate tail git-multibranch:11 changed P0→P1, accepted P cache6784→640;
TTFT11.6065→20.2424. Delta8.6359 decomposes into before-P+2.4153,
P API+2.9455 and P-to-D+3.2675, Dfirst+0.0076s. P1 had actual overlap with
distribution-search:12; D0 was processing decode iterations. Nominal first
latency instead fell79.0520→76.9846 because causal-ready was10.7032s earlier.
Do not call all intervals pure queue or add overlapping iterations again.
Default low total route CPU58.1168→64.4998s despite fewer full calendars625→621;
boundary full calendars207→273. A faster fixed-state removal does not imply
cheaper overall control after state/branch changes, and measured host timing
is not independently isolated by one sequential pair.

Default middle's old worst git-multibranch:10 changed D1→D0 and P cache640→6784;
P API7.5815→4.7917, Dfirst10.8864→0.6703, while P-to-D wait0.7277→4.5306.
This clears the old tail; the new maximum is feal:12 at19.0700s.
Default high's old worst dna-insert:11 preserves P API1.1048 and P cache8320,
but before-P7.0577→1.3813 and Dfirst9.9009→0.4559 after D1→D0; P-to-D
2.8644→5.5007 partly offsets it. The new maximum is fix-git:12 at15.6260s.
High makespan492.8660→497.0778 and nominal p99387.8029→390.6816 worsen:
conventional TTFT gains do not imply all completion measures improve.

Sharing middle regresses although total accepted P cache306560→350464 tokens:
dna-insert:12 before-P4.0131→6.8518 and P-to-D2.3651→9.3102 dominate;
its own P prefix9472→7040 differs from the aggregate trend. Sharing high's
headless-terminal:11 TTFT28.6195→13.9892 mainly reflects P-to-D14.4511→1.0191,
but its absolute first173.8276→173.9827 is nearly unchanged because ready is
14.7855s later. Explicitly preserve both conventional and nominal metrics.

One apparent13.0525s original-forecast error is not unexplained cost: low-rate
fix-code-vulnerability:11 is moved after P. Original first370.4867/D0 changes
at advice to first384.0949/D1, actual383.5392. Keep prediction373.4770 would
be earlier for that request, but current pending maximum21.0849→20.2446 makes
move win. This is an intentional local tradeoff and a possible finite-horizon
limitation, not proof the move caused final p99 harm. All alternatives refer
to that snapshot, not observed counterfactual outcomes. See next ablation.

Full evidence: evidence/forecast-feedback-20261027/prefix-reclaim-final-summary-03.json,
paired charged analyses and raw archives. Default archive SHA
9ae7aae8db2a5560a87900d558bcbcca72d4d78a20429200f61162b7b2b79882;
sharing SHA e8fde03439e652df99cee9bb7d9ad3e8dac70bee1cc66a22327cffc13af83890.
Next: no-decode-advice-20261027.md. This tests the marginal value of optional
post-P rerouting without changing initial routing or assuming RDMA congestion.
