# Shared policy research implementation

Start with [the research handoff](../issue23_handoff/README.md), including metric,
baseline and evidence limits. This directory is an isolated research overlay;
it does not replace the installed serving connector or native engine.

- `simulate_policy_feedback.py`: three-rate causal-feedback CPU screening.
- `runtime/code/shared_pair.py`: six fresh arms, B/T at rates 300, 150, 600.
- `runtime/code/audit_shared.py`: candidate-specific result/source/receipt audit.
- `runtime/overlay/pegaflow/`: immutable live-parent dependencies.
- `runtime/calibration-seed/`: original 32 isolated requests / 64 stages.
- `runtime/workload/`: frozen replay and source-selection manifest.
- `../../python/pegaflow/strategies/`: common native, pending-tail and explicitly
  named MemServe/Mooncake strategy adapters. The latter are not full systems.

The snapshot's `source.sha256` and `confirmation-sources.json` apply to the
original 2026-10-16 runtime files only. The portable simulator entry point was
extended for this handoff, outside that immutable runtime. No completed new
native-baseline versus pending-tail real A/B was verified at handoff.

For a fresh clone's CPU run:

```bash
PYTHONPATH=python python experiments/issue23_strategies/simulate_policy_feedback.py \
  --input-bundle experiments/issue23_handoff/simulation-inputs.json \
  --output /tmp/tailguard-screen-unique.json
```

Live launchers retain historical `/root` paths and endpoints. They are not
portable autostart scripts. Inspect ownership, output absence, allocation,
runtime/native hashes and current addresses first, stage a new immutable job,
then use a bounded supervisor. Never overwrite or blindly rerun an old job.
