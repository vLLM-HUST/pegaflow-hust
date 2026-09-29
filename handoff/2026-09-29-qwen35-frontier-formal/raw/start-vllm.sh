#!/usr/bin/env bash
set -eo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 RUN_DIR" >&2
  exit 2
fi

run_dir=$1
mkdir -p "$run_dir"
date -u +%FT%TZ > "$run_dir/server-start-utc.txt"
npu-smi info > "$run_dir/npu-before-server.txt"
cp /root/pegaflow-qwen35-qualification/server-metadata.json "$run_dir/server-metadata.json"
cp /root/frontier-kvcompress-runs/model/modelscope-712cf743-weights.SHA256SUMS "$run_dir/model-weights.SHA256SUMS"
cp /root/frontier-kvcompress-runs/model/weight-verification.txt "$run_dir/model-weight-verification.txt"
cp /root/pegaflow-qwen35-qualification/manager-config.json "$run_dir/manager-config.runtime.json"
git -C /root/vllm-hust rev-parse HEAD > "$run_dir/vllm-head.txt"
git -C /root/vllm-hust status --short > "$run_dir/vllm-status.txt"
git -C /vllm-workspace/vllm-ascend rev-parse HEAD > "$run_dir/vllm-ascend-head.txt"
git -C /vllm-workspace/vllm-ascend status --short > "$run_dir/vllm-ascend-status.txt"
git -C /root/GenNova_OSDI_paper/pegaflow-hust rev-parse HEAD > "$run_dir/mod-head.txt"
git -C /root/GenNova_OSDI_paper/pegaflow-hust status --short > "$run_dir/mod-status.txt"
git -C /root/GenNova_OSDI_paper/swe-prefix-reuse rev-parse HEAD > "$run_dir/workload-tool-head.txt"
sha256sum /root/frontier-kvcompress-runs/workload/qwen35-prepared.json > "$run_dir/prepared-workload.sha256"
curl -fsS http://127.0.0.1:9091/health > "$run_dir/pegaflow-health-before-server.txt"
curl -fsS http://127.0.0.1:9091/metrics > "$run_dir/pegaflow-metrics-before-server.txt"

export PYTHONPATH="/root/vllm-hust:/vllm-workspace/vllm-ascend:/root/GenNova_OSDI_paper/vllm-hust-dev-hub/scripts/frontier_pipeline:/root/GenNova_OSDI_paper/vllm-hust-dev-hub/scripts/frontier_runtime:${PYTHONPATH:-}"
python - <<'PY' > "$run_dir/import-paths.txt"
import pegaflow
import vllm
import vllm_ascend

print(f"vllm={vllm.__file__}")
print(f"vllm_ascend={vllm_ascend.__file__}")
print(f"pegaflow={pegaflow.__file__}")
PY

source /usr/local/Ascend/ascend-toolkit/set_env.sh
source /vllm-workspace/vllm-ascend/vllm_ascend/_cann_ops_custom/vendors/custom_transformer/bin/set_env.bash
export ASCEND_RT_VISIBLE_DEVICES=2,3
export PYTHONHASHSEED=0
export VLLM_VERSION=0.25.1
export TASK_QUEUE_ENABLE=1
export XDG_CONFIG_HOME=/root/pegaflow-qwen35-qualification/manager-state

exec vllm-hust-ext run --shutdown-grace-seconds 60 -- \
  python -m vllm.entrypoints.cli.main serve \
  /root/models/Qwen3.5-35B-A3B \
  --host 127.0.0.1 --port 33784 \
  --served-model-name frontier-qwen35-unified \
  --tensor-parallel-size 2 --pipeline-parallel-size 1 \
  --distributed-executor-backend mp --worker-cls pipeline_worker.Worker \
  --dtype bfloat16 --kv-cache-dtype auto --max-model-len 262144 \
  --max-num-seqs 16 --max-num-batched-tokens 4096 \
  --gpu-memory-utilization 0.95 --seed 17 --enable-prefix-caching \
  --mamba-cache-mode align --enable-prompt-tokens-details --async-scheduling \
  --shutdown-timeout 60 --additional-config '{"enable_cpu_binding":false}' \
  --limit-mm-per-prompt '{"image":0,"video":0}' \
  --compilation-config '{"cudagraph_mode":"FULL_AND_PIECEWISE","cudagraph_capture_sizes":[3,6,12,24,48],"max_cudagraph_capture_size":48}' \
  --speculative-config '{"method":"mtp","num_speculative_tokens":2}' \
  --generation-config vllm \
  --override-generation-config '{"temperature":0.0,"top_p":1.0,"top_k":-1,"presence_penalty":0.0}' \
  --default-chat-template-kwargs '{"enable_thinking":true}' \
  --kv-cache-memory-bytes 26038239232
