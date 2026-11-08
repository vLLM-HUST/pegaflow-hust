# Serving artifact custody

The 42 checked-in `results/**/trace_audit.json` files are historical diagnostic
inputs, not formal performance evidence. They reference 17 PegaFlow execution
commits. The current rebuilt repository does not retain long-lived refs for
those exact executions. Twelve commits were already known to differ from the
later main-tree replacements; the three former SAME-TREE mapping candidates
also no longer have a durable ref in this repository. No mapping is therefore
promoted by inference.

`evidence/custody/artifact-policy.json` classifies all 42 records as
`exploratory/history-only`. The list is executable rather than editorial:

```bash
python3 scripts/verify_artifact_custody.py --offline
```

The checker fails when an artifact is added, removed, ambiguously classified,
or unreadable. A future record may enter `formal` only with exact, retrievable
PegaFlow/vLLM/vLLM-Ascend commit refs; a SHA-256 model config; CANN, torch-npu,
and device mapping; the exact checked-in Python reproduce command; and a
non-empty list of raw logs with SHA-256. Online mode resolves every declared ref
with `git ls-remote`; `--offline` skips only that network lookup and retains all
local content checks.

This classification does not delete or rewrite old data. The 2026-08-13
break-even observation remains useful as candidate selector evidence, but it is
not a formal endpoint, paper, or cross-system performance result. New selector
or fused-carrier claims must be rerun from a retained current commit and added
to the policy only after the checker accepts the complete record.
