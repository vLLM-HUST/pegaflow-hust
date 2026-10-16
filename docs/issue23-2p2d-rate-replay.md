# Qwen2.5 2P2D rate replay

This experiment runs two TP1 producer replicas and two TP1 consumer replicas on
two hosts. Each host has two NPUs. An open-loop Poisson client controls the total
request rate. Producers alternate by request index; consumers are independently
selected with probability 0.5 before execution. Orders, arrival intervals and
routes are paired across rates within each seed.

## Workload

Dataset: `openguardrails/terminal-bench-2.1-deepseek-v4-flash-trajectories`, revision
`3f5b93ff1c5ea84b2c94cf479d0c942227191b4a`, directory `trajectories/terminus2`.

| Split | Task | Inclusive ATIF agent steps | Count |
|---|---|---|---:|
| Main | distribution-search | 2–15 | 14 |
| Main | dna-insert | 2–16 | 15 |
| Main | feal-differential-cryptanalysis | 2–14 | 13 |
| Main | fix-code-vulnerability | 2–11 | 10 |
| Main | fix-git | 2–14 | 13 |
| Main | git-leak-recovery | 2–7 | 6 |
| Main | git-multibranch | 2–12 | 11 |
| Main | headless-terminal | 2–14 | 13 |
| Calibration | adaptive-rejection-sampler | 2–19 | 18 |
| Calibration | code-from-image | 2–15 | 14 |

`prepare_trajectories.py` maps previous ATIF user/system messages, assistant
messages and tool calls, and ordered tool observations to Qwen messages. It omits
the target step and the separately stored reasoning. Tool argument keys are
normalized before applying the model chat template with
`add_generation_prompt=True`. Each request is an independent history snapshot;
generated answers do not modify later snapshots.

Input audit with the proposed Qwen snapshot
`a09a35458c702b33eeacc393d103063234e8bc28`:

| Split | Requests | Mean input tokens | Max input tokens |
|---|---:|---:|---:|
| Main | 95 | 9919.23 | 25140 |
| Calibration | 32 | 12389.69 | 30800 |

The run uses BF16, a 32768-token context and 256 generated consumer tokens with
`ignore_eos=True`. Producers generate one internal token to drive prefill and
cache save; producer output is not forwarded to the client. A unique cache salt
is shared by the selected P and D for each request. Cache reuse between distinct
requests is isolated without modifying prompt tokens.

## Deployment

`rate_deployment.py` accepts a JSON config with:

- `root`, `model_path`, `model_revision`, `python`, `site_packages`, `bin`;
- `meta_host`, `meta_port`, `meta_http_port`;
- `host_pool_bytes`, `kv_cache_bytes`, `max_num_seqs`;
- `instances`: four objects with `id`, `role` (`P`/`D`), `node`, `device`, `host`,
  `model_port`, `engine_port`, `engine_http_port`.

Use distinct ports and engine processes. Model visibility selects one NPU. Engine
device scopes use the corresponding global runtime device ID with unrestricted
engine visibility, matching the connector's IPC device identity.

The source uses NPU→CPU save, CPU memory RDMA transfer, then CPU→NPU restore.
`PEGAFLOW_SYNC_SAVE=1` makes the engine Save RPC wait for insertion and MetaServer
publication. `PEGAFLOW_SYNC_SAVE_ON_FINISH=1` makes the worker finish marker wait
for its saves. Both are required for the request-local handoff contract.

Normal serving is used for this pipeline. The frozen Issue23 experiment mode has
different cache publication and hidden-snapshot semantics; this runner obtains
RDMA timing from normal-path engine logs.

Load the CANN environment before starting services and preserve its `PYTHONPATH`.
Example on each host, using the actual config path and node name:

```bash
python scripts/issue23/rate_deployment.py --config config.json --node 81 meta
python scripts/issue23/rate_deployment.py --config config.json --node 81 engines
python scripts/issue23/rate_deployment.py --config config.json --node 91 engines
python scripts/issue23/rate_deployment.py --config config.json --node 81 models
python scripts/issue23/rate_deployment.py --config config.json --node 91 models
python scripts/issue23/rate_deployment.py --config config.json --node 81 resources
python scripts/issue23/rate_deployment.py --config config.json --node 91 resources
```

Commands run on the named host; start the one MetaServer on its configured host.
`--instance P1` can select a single instance. The deployment retains its commands,
PIDs and logs under `services/`.

## Prepare and run

```bash
python scripts/issue23/prepare_trajectories.py \
  --snapshot /path/to/fixed-dataset-snapshot \
  --tokenizer /path/to/Qwen2.5-7B-Instruct \
  --output prepared/requests.jsonl

python scripts/issue23/rate_replay.py \
  --config config.json --requests prepared/requests.jsonl \
  --output runs/campaign --mode campaign --seeds 17,29,43 --budget 900
```

The campaign checks the four routes with held-out inputs, calibrates using all 32
held-out requests at bounded in-flight concurrency 4 and 16, then runs the 95 main
inputs at 0.50, 0.85 and 1.10 times the workload-adjusted reference rate. Each
completed point is saved immediately. Existing completed points can be resumed.
Calibration latency includes its explicit client concurrency queue and is
reported separately from open-loop main latency.

The main load generator does not wait for previous responses. It records
dispatch lag and uses a bounded admission count of 64. A separate per-producer
byte reservation limits pending handoffs to 75% of that producer's host pool;
the reservation is released when D produces its first token. Its wait is
measured separately. Memory exhaustion and failed requests remain in results.

`--mode point --rate <global-rps> --label <point>` runs one additional full-95-input
point. `--mode calibration` and `--mode smoke` are available for preparation.

## Measurements and interpretation

- `requests.jsonl`: dispatch lag, P API span, visible-save observation, D first
  token, final usage, client chunk timings and terminal status.
- `events.jsonl`: correlated admission, submit, visibility and completion events.
- Per-point `cache-paths.json`: actual external matches, including immediate-ready
  matches logged by the connector and asynchronous query-ready events.
- Worker telemetry: actual scheduled tokens, active sequences and host execute
  spans for attention and hybrid models, without extra device synchronization.
- Engine logs: source, transferred MiB, descriptor count, connection, query,
  allocation, submission, RDMA completion wait and rebuild durations.
- Resource samples: per-model queue counters, CPU quota/throttling, memory,
  RNIC counters and per-card AI Core/HBM samples. Regular samples retain selected
  metric series; full metrics snapshots are saved before and after each point.

`post→completion` contains endpoint and transport effects. The two consumers
share one physical RNIC; logical consumer convergence alone does not establish
multi-host fabric incast. Distinguish NPU idle periods from compute-active
periods and decode's memory-intensive small batches. Report observed errors,
fallbacks and cache capacity limits independently of numerical output changes.

For Qwen BF16 TP1, the theoretical full-prompt KV is 57344 bytes/token, averaging
568.8 MB/request for these inputs. Link demand must be compared with measured
payload capacity; the main run does not apply a bandwidth cap or background
competitor load.

```bash
python scripts/issue23/analyze_rate_replay.py \
  --root /path/to/experiment --campaign campaign \
  --output /path/to/analysis --plot
```

The report includes per-request stage CSV, p50/p95 latency, rate/throughput,
cache-path counts, RDMA stages and NPU samples. Ninety-five unique snapshots per
point provide exploratory tail observations. Larger numbers of repeats should
be used before making stable p99 claims.
