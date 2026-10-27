# Optional post-P advisory ablation

Status: FINAL, all18 full-cost CPU arms finished and independently archived /
audited. Mixed results; do not promote disabling all optional advice. The old
launch checkpoint below is history. Current continuation: advice-window-20261027.md.

## Evidence and hypothesis

Prefix02 passed six exact mechanism pairs, but all measured control charged
changed the causal arrivals, resource interleaving and cache history. Against
same-round exact-calendar02, default three-rate p99 reductions were
-6.2793%, +8.6443%, +25.3343%; unmeasured sharing1 sensitivity was
-10.1168%, -18.8274%, +39.0086%. This is not a robust winner.

Default low-rate fix-code-vulnerability:11 initially selected D0 and forecast
first370.4867; later advice intentionally moved it to D1. The advice predicted
keep first373.4770 versus move384.0949, but selected the latter because current
pending maximum fell21.0849 to20.2446. Actual first383.5392 is close to the
revised prediction. Calling the original13.0525s endpoint residual a missing
queue model would therefore be wrong. The move sacrifices this request to help
the current pending maximum, without observing later session releases. It does
not prove that the move harmed the final p99: no same-state no-move execution
was observed. Default low-rate git-multibranch:11 is the actual maximum and
exhibits both a different P cache choice and longer P-to-D waiting.

Hypothesis: the optional late re-optimization can have limited value after
full initial P/D cache-aware routing, while its serialized CPU/RPC and changes
to release/cache order can offset local predicted gains. Disable this mechanism
alone to identify its marginal contribution before changing the objective or
inventing a future-session predictor. This is an ablation, not an assumption
that all optional actions are bad, nor a new all-rate winner.

## Intervention and accounting

NoDecodeAdviceSimulation inherits the entire frozen prefix parent, disables
only the launch of optional post-P D advice with an instance flag. The shared
route function still executes original joint initial routing. P-head boundary
checks, D admission/boundary control, accepted cache, source/target gates,
H2D, full decode, synchronous save, release ordering and all remaining control
costs are unchanged. Costs disappear only because the optional operation is
not executed; no accounting subtraction or budget change occurs.

Same-round parent and candidate share the new common control file; parent flag
defaults on. Three fixed rates, 95/8 original input/seed/capacities and default0
plus sharing1 sensitivity remain frozen. No zero-cost equivalence matrix is
appropriate because this intervention intentionally changes policy. Two scoped
switch contracts plus the five existing yielding tests pass; full matrices
must verify actual reference advice, candidate zero advice, all95 actual D
routes equal their initial selection,190 stages and nonzero D control work.
The existing source/history/workload/release audits remain required.

Analyze all requests, not just moves: controller time changes later arrivals
even when no move commits. Report conventional and nominal first latency,
full completion, queues, accepted cache, original and revised forecasts. Do
not sum overlapping device iterations into gaps or call all gaps pure queue.
If mixed, retain the result and locate first useful/unhelpful interventions
before designing a past-only objective extension. No task/rate exception,
gain threshold, forced balance or future oracle is introduced.

## Actual launch checkpoint

/root/no-advice-pipeline-01.py is background PID/PGID402862, started
1793074541.613639, working directory /root/issue23-no-advice-source-01.
Output /root/issue23-no-advice-charged-pipeline-01, default and sharing01
separate. 4500s overall/1800s each, three workers, BLAS/OMP1,19 bounded steps.
Previous prefix03 completed all17 steps before launch; no other matrix alive.
Source SHA c58d7e557029a9d3759c8fad8aa5eb0a2354e2fa52795f3d21b01563d3bbcaf6;
runner SHA66e7479a3e5b5bcf3398cc735a13e7b255784d61e10fbdf6f9749b9e54c75371.
Read actual start/result/console before any continuation; no duplicate run.
Results pending, current version is a diagnostic ablation, not a promoted win.

## FINAL full-cost evidence

The initial pipeline finished the default9 arms and every source/work/history/
activation audit, then its offline summary failed because a helper was omitted
from the source package. The failure is preserved. A separate one-file analysis
support package repaired the offline import, verified every default raw JSON
unchanged, and executed only the never-started sharing1 matrix. No default rerun
and no mutation of the frozen algorithm occurred. Recovery completed10 bounded
steps with exit0/no stop. All18 arms retain full decode and cost accounting.

| Scenario | Rate | Advice ON p99(s) | OFF p99(s) | Reduction |
|---|---:|---:|---:|---:|
| Default0 |0.2452|19.371552|17.124333|11.60%|
| Default0 |0.4903|20.647869|17.085838|17.25%|
| Default0 |0.9806|17.586300|19.255883|-9.49%|
| Sharing1 sensitivity |0.2452|17.293862|15.169915|12.28%|
| Sharing1 sensitivity |0.4903|17.897742|19.264517|-7.64%|
| Sharing1 sensitivity |0.9806|15.497171|16.303422|-5.20%|

These compare the same-round prefix02 parent, not native or the retained
TimedMemo default. All controller/fit/iteration/planning/migration costs remain
on the causal timeline. Sharing1 is unmeasured sensitivity, not an NPU result.
All95 requests are exposed; nearest-rank p99 is max, not stable significance.

### Observed behavior and limits of attribution

Default ON searched45/46/38 times but committed only3/3/1 moves; OFF searched0.
Saving optional CPU10.907/9.229/6.153s did not guarantee lower total work: in
high rate route CPU61.254→69.841s and D boundary33.455→39.931s, P restored
tokens538368→372096, D122368→80768. The most degraded fix-git:9 switched
P1/D0→P0/D1, P recovery7680→640, P API0.617→3.746s and D API-to-first
0.377→5.432s. Its TTFT3.563→14.298s demonstrates downstream cache/compute
feedback, not a direct subtraction of advisory CPU or measured RDMA congestion.

Default high's new maximum feal:13 kept P1/D0, but P recovery8320→6784,
before-P0.655→2.447s, P API8.066→8.776s, P-to-D4.688→6.889s; TTFT14.520→
19.256s. Original selected forecast residual was only0.326s in OFF. This is
not evidence that all of the regression is missing queue prediction. Default
middle improved the old worst feal:12 using11776 rather than640 P tokens,
reducing P API10.530→5.474s; whole-arm mean still slightly worsened.

Sharing ON searched6/11/14 times with ZERO committed moves. OFF still changes
later timing, initial routes and caches. Sharing middle's worst feal:11 moved
P0→P1, kept640 P tokens, and before-P grew4.911→8.197s; P API9.709→9.844s
is almost unchanged. This is predominantly a different P waiting path. The
largest dna-insert:9 regression preserved8320 P tokens and P API0.291s but
before-P grew3.407→14.277s. Total optional search costs are not this wait.

The first changed INITIAL route in default middle is feal:4 at62.811/62.803s,
BEFORE first ON advice86.010s. Sharing high also diverged before any advice.
Measured host CPU differences can already change queues/cache state before the
intervention. Therefore these are closed-loop full-cost observations, not an
isolated causal estimate of disabling advice. Even the observed gains cannot
all be assigned to the switch. The new chronological analysis records this
explicitly for all570 aligned request comparisons (95×six cases).

Committed moves sometimes intentionally delay their own first token to reduce
the current pending maximum. For default high distribution:12, keep predicted
342.5442 versus move350.8149, pending max19.6066→18.1781. A locally valid
tradeoff is not a theorem about final exposed-replay p99. Future causal arrivals
and cache paths remain unknown. No fairness quota or task-specific exception
is justified by these examples.

### Continuation and receipts

Test a completed-only remaining slot-release window against optional search
CPU+RPC, rather than turning every search off. Preserve unknown-state searches
and charge the trigger itself. See advice-window-20261027.md and version registry.

Full archive on P/root and Windows SHA verified:
- no-advice-completed-default-01-full.tar.gz:
  ddd24d035a331fe5a2ece838e74e64433bfa66fb5ba7999f235a7796034ae852
- no-advice-completed-sharing-01-full.tar.gz:
  37491b391dd88c9d2b13af927369a2a4df0f37031d426199f6b8ee01f726ce6f
- Original analysis failure plus raw default:
  91e60b5d3a1a7c540a3a52bda76b8d2d651167aecff53e37b86457efe1063e18

Complete raw evidence remains in cache-profile-20261026/result-no-advice-*-01
and the original/root directories. Repository evidence includes full request
alignment, chronology, audits and recovery receipt; do not overwrite old results.
