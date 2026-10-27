# Optional search versus slot-release window

Status: scoped CPU candidate advice-release-window-01; full-cost tests pending.
Parent/reference is prefix-reclaim-calendar-02, not TimedMemo or native serving.
Read the registry for actual launch/result status. No hardware gain claim.

## Evidence motivating this intervention

Disabling optional post-P D advice completely gave default three-rate p99
reductions +11.6006%, +17.2513%, -9.4937%, and unmeasured sharing1 sensitivity
+12.2815%, -7.6366%, -5.2026%. Do not promote the disabled version. The sharing
reference made 6/11/14 searches but zero committed moves. In default high rate,
disabling advice cut actual P restored tokens 538368 to372096 and worsened
TTFT despite saving6.153s optional control. These are closed-loop observations,
not proof that any one move caused the final tail. Default middle diverged in
initial routing before the first optional search; host control timings are a
confounder and a causal simulator input, not isolated algorithm treatment.

The tested next hypothesis is narrower: some optional searches will not finish
before the blocked head acquires its original slot. Such a search cannot commit
under the existing ownership contract, yet still occupies the route controller.
Use the existing release model to predict that opportunity window; preserve
search when the window is unknown or sufficient. No outcome-fitted margin.

## Trigger and limits

For an observed FIFO head with all D slots occupied by post-first decode and
no external device-gate owner, let c_j be observed decoded tokens and N_j the
fixed output budget. Existing completed-only stage fits give decode tail d_j
and final synchronous save s_j. The full remaining predecessor estimate is

    r_j = d_j (N_j-c_j)/(N_j-1) + s_j.

If an iteration is already issued, use only its observed member/counts and
completed cost fits: include the whole predicted non-preemptible iteration,
then subtract its token progress from each participating predecessor before
computing r_j. Do not read sampled Iteration.seconds or a future completion.
Counting the whole issued iteration is conservative because elapsed service
is censored. The estimated window W is the minimum resulting release time.

Let C be mean CPU wall time of previously completed full advice searches in
this arm plus existing boundary RPC. Skip this one optional attempt if W<=C.
There is no arbitrary gain threshold, task/rate feature or forced balancing.
Cold control history, mixed phases, non-head FIFO work, or an externally held
gate retain the original advisory path. Future arrivals/gate acquisitions can
invalidate a window estimate; this is a testable trigger, not a deadline proof.

All trigger observation, model-freeze and computation is measured and advances
the serialized control timeline, including rejected attempts. Skips never enter
completed-search CPU history. Allowed searches retain the original complete
candidate set, pending-first objective, cancellation and ownership checks.
H2D, full decode, sync-save, cache capacity/leases, copying, all other planning,
source/destination gates and final drain are unchanged and fully charged.

## Evaluation contract

Freeze source; use the same95 requests/8 causal sessions, original costs,
capacities, arrivals and seed. Compare same-round parent at all three rates
under default0 then sharing1 sensitivity, three CPU workers maximum. Fourteen
local tests cover history exclusion, sync-save, issued work, uncertainty fallback,
charged trigger and original advisory/switch contracts. Independently reconstruct
trigger costs and decoded progress from completed observations/iteration history;
check actual skip activation separately from p99. Keep all mixed/negative output.
If no searches skip, this candidate has not tested the hypothesized mechanism.
No new zero-cost equivalence requirement: the policy intentionally changes.

## Actual execution checkpoint

Source01 SHA caa91cd838efe30be16628052e364c03ed412ac76b6f1e7f52d55bdc1caa33dc,
208 files. Fourteen targeted tests passed locally and on P; offline imports pass.
Launcher01 failed before samples (incorrect predecessor path), retained archive
8be1383c3cb526f137dd60eec15106d7af6373e10e85a18d57f8220f17ff5b27.
Launcher02 accidentally retained old --policy no-decode-advice and its old audit.
It ran9 CPU arms, but ZERO intended candidate arms. All evidence preserved in
advice-window-wrong-policy-02-full.tar.gz, SHA
92dc4bea6fe299b644e20ecccfd36ec1be5cc581e06c9cd2aa8c0a4081004bdc.
Do not present its p99 as this candidate. Recovery03 prelaunch guard rejected the
actual11-step prior failure; it never launched. No frozen algorithm was patched.

Correct explicit launcher04 imports the candidate class and correct auditor,
records exact --policy advice-window, and asserts actual result revision before
analysis. PID/PGID464967, started1793079111.81262, command stable Python plus
/root/advice-window-pipeline-04.py; cwd/root/issue23-advice-window-source-01.
Runner SHA d1b060b24443bc070e1a7994c883f2bc35b09d7c30c81f672b46d90728ca1386.
Root /root/issue23-advice-window-charged-pipeline-04; default-02 then sharing-02.
4500s total/1800s each,3workers/OMP-BLAS1. Check actual result/console and owned
process before progressing. Initial recipe is a full-cost simulation, no real NPU.
Do not overwrite or rerun completed outputs. Hourly research remains ACTIVE.


## FINAL: 18 intended arms complete, mixed, not promoted

Launcher04 exited successfully. All source/input/history/work-conservation,
release-estimate, boundary and trigger audits passed. Both full archives are
independently SHA-verified in /root and Windows; the receipt and all570 paired
request paths are in evidence/forecast-feedback-20261027. Earlier launcher
failures/wrong-policy outputs remain separate and unmodified.

| Scenario | Rate | Parent p99 s | Candidate p99 s | Reduction | Skips/checks |
|---|---:|---:|---:|---:|---:|
| default0 | .2452 | 19.0495 | 19.5157 | -2.45% | 2/44 |
| default0 | .4903 | 15.3654 | 20.3996 | -32.76% | 1/37 |
| default0 | .9806 | 25.3660 | 22.7085 | +10.48% | 0/36 |
| sharing1 sensitivity | .2452 | 18.3766 | 15.4517 | +15.92% | 1/8 |
| sharing1 sensitivity | .4903 | 17.6263 | 18.4783 | -4.83% | 1/13 |
| sharing1 sensitivity | .9806 | 17.7896 | 20.4827 | -15.14% | 2/13 |

These are charged CPU development-replay measurements, conventional TTFT,
95 samples / nearest-rank p99=max. Default high has ZERO skips; its gain does
not demonstrate this trigger. Default middle/high and sharing high differ in
initial routing before their first trigger. Sharing middle differs before its
first skip. Whole-run differences cannot isolate the cancellation effect.

Observed default-middle feal:14 stays P0/D0 but TTFT10.2492->20.3996s:
before-P .8083->3.9293, P-API7.5704->8.6747, P-to-D .7157->6.5814;
P accepted cache9216->6784, D1408->0. Default-high fix-code-vulnerability:9
has MORE P cache10880->14080 and lower P-API3.5393->2.2906, yet before-P
1.1382->8.7952 and P-to-D3.8099->10.8122. Cache alone does not explain tails.

Specific model omission: sharing-high fix-code-vulnerability:6 estimates a
.259995s release window but acquires D after1.305898s. The already-issued
initial route of git-multibranch:6 (origin60.171684, CPU1.755996, sourceD0)
precedes trigger62.438641. Its history copy holds gate2/gate0/link:dp at
62.445147..63.540908 (1.095761s), between decode iterations, delaying D0's
next release. This is source-device gate competition, not measured RDMA
bandwidth saturation. It was known pending work, not a future-arrival oracle.

Same-destination window MAE in default low/mid/high is .1063/.1018/.1149s;
sharing .0258/.0199/.2019s. Errors compare to actual request acquisition,
so include later interference/control and are not pure compute-model errors.
Most estimates are close; only7/151 attempts skip. Do not inflate penalties
or force actions. Next: reuse the existing incumbent resource DAG to price
actual FIFO acquisition and compare remaining search+commit cost after the
first forecast, preserving full cost and independent provenance checks.
