#!/bin/bash
# T6: DeepSeek-V4-Flash TP8 + PegaFlow connector 启动脚本 (独立于 deepseek-v4-deploy)
#
# 用法:
#   bash /workspace/HUST/t6-v4-serve.sh              # PegaFlow on (T6 主臂)
#   bash /workspace/HUST/t6-v4-serve.sh --no-pegaflow  # PegaFlow off (对照臂, KV 每 rank 冗余)
#
# 环境: deepseek-v4-deploy (vllm v0.27.1 + vllm-ascend main, torch 2.11, CANN 9.0)
# 参考: deepseek-v4-deploy/run_vllm_serve.sh (已验证部署) + pegaflow connector (兼容 vllm 0.27 抽象方法 ✓)
set -e
cd /workspace/HUST

PEGAFLOW_HUST=/workspace/HUST/pegaflow-hust
MODEL=/workspace/HUST/models/DeepSeek-V4-Flash-w8a8-mtp
# 可被环境变量覆盖: 四臂验证 (native / isolated-share / always-share) 需
# 每域独立 server (端口+池) 或共享单 server。
PEGAFLOW_PORT=${PEGAFLOW_PORT:-50080}
PEGAFLOW_POOL=${PEGAFLOW_POOL:-16gb}
VLLM_PORT=${VLLM_PORT:-8900}
MAX_MODEL_LEN=${MAX_MODEL_LEN:-133120}
NO_PEGAFLOW=0
[ "$1" = "--no-pegaflow" ] && NO_PEGAFLOW=1

source /root/miniconda3/bin/activate deepseek-v4-deploy

# ---- CANN 9.0 (torch-npu 2.11 已验证组合; 9.1-beta.3 会崩) ----
export ASCEND_HOME_PATH=/usr/local/Ascend/cann-9.0.0
export ASCEND_TOOLKIT_HOME=$ASCEND_HOME_PATH
export LD_LIBRARY_PATH=$ASCEND_HOME_PATH/aarch64-linux/lib64:$ASCEND_HOME_PATH/aarch64-linux/lib64/plugin/opskernel:$LD_LIBRARY_PATH
export PYTHONPATH=$ASCEND_HOME_PATH/python/site-packages:$PYTHONPATH

# ---- 文档要求 env ----
export PYTHONHASHSEED=0  # PegaFlow 跨实例 KV hash 一致性必需 (connector 启动警告)
export OMP_PROC_BIND=false
export OMP_NUM_THREADS=10
export PYTORCH_NPU_ALLOC_CONF=expandable_segments:True
export LD_PRELOAD=/usr/lib64/libjemalloc.so.2:$LD_PRELOAD
export HCCL_BUFFSIZE=1024
export TASK_QUEUE_ENABLE=1
export HCCL_OP_EXPANSION_MODE="AIV"
export TORCHINDUCTOR_AUTOGRAD_CACHE=0

cleanup() {
  echo "cleaning up..."
  pkill -9 -f "VLLM::" 2>/dev/null || true
  [ -n "$SERVER_PID" ] && kill -9 $SERVER_PID 2>/dev/null || true
  pkill -9 -f "pegaflow-server --addr" 2>/dev/null || true
}
trap cleanup EXIT

# ---- 显存探测 (权重 ~35GiB/卡) ----
MIN_FREE=$(python - <<'PY'
import torch, torch_npu
frees = []
for d in range(8):
    torch.npu.set_device(d)
    free, total = torch.npu.mem_get_info()
    frees.append(free / 2**30)
print(f"{min(frees):.1f}")
PY
)
# vLLM 需要 GMU × 卡显存 + 权重余量; 探测门槛按 gmu 推导 (不能固定 40,
# 共享机显存波动时 40 会放行但 vLLM 起不来)。
HBM_TOTAL=60.96
NEED=$(python3 -c "print(f'{${GMU:-0.78} * $HBM_TOTAL + 3:.1f}')")
echo "min free HBM: ${MIN_FREE}GiB (need >= ${NEED})"
if (( $(echo "$MIN_FREE < $NEED" | bc -l) )); then
  echo "ERROR: not enough HBM" >&2
  exit 1
fi

# ---- pegaflow-server (仅 PegaFlow 臂) ----
if [ "$NO_PEGAFLOW" = "0" ]; then
  SERVER_BIN=$PEGAFLOW_HUST/target/debug/pegaflow-server
  echo "starting pegaflow-server on :$PEGAFLOW_PORT ..."
  $SERVER_BIN --addr 0.0.0.0:$PEGAFLOW_PORT --pool-size $PEGAFLOW_POOL \
      --devices 0,1,2,3,4,5,6,7 --log-level info > /tmp/t6-server.log 2>&1 &
  SERVER_PID=$!
  for i in $(seq 1 180); do
    ss -tln | grep -q ":$PEGAFLOW_PORT" && break
    sleep 1
  done
  ss -tln | grep -q ":$PEGAFLOW_PORT" || { echo "pegaflow-server failed to start"; tail -5 /tmp/t6-server.log; exit 1; }
  echo "pegaflow-server ready"
fi

# ---- vllm serve ----
KV_ARGS=()
if [ "$NO_PEGAFLOW" = "0" ]; then
  KV_CFG='{"kv_connector": "PegaKVConnector", "kv_role": "kv_both", "kv_connector_module_path": "pegaflow.connector", "kv_connector_extra_config": {"pegaflow.mode": "read_write", "pegaflow.transfer_backend": "ascend_direct"}}'
  KV_ARGS=(--kv-transfer-config "$KV_CFG")
  export PEGAFLOW_HOST=http://127.0.0.1
  export PEGAFLOW_PORT=$PEGAFLOW_PORT
  export PYTHONPATH=$PEGAFLOW_HUST/python:$PYTHONPATH
fi

# No exec: the EXIT trap must survive to clean up pegaflow-server when vllm exits.
vllm serve $MODEL \
    --max-model-len $MAX_MODEL_LEN \
    --max-num-batched-tokens 8192 \
    --served-model-name dsv4 \
    --gpu-memory-utilization ${GMU:-0.78} \
    --max-num-seqs 32 \
    --data-parallel-size 1 \
    --tensor-parallel-size 8 \
    --enable-expert-parallel \
    --tokenizer-mode deepseek_v4 \
    --tool-call-parser deepseek_v4 \
    --enable-auto-tool-choice \
    --reasoning-parser deepseek_v4 \
    $([ "${PREFIX_CACHE:-0}" = "1" ] && echo "" || echo "--no-enable-prefix-caching") \
    --model-loader-extra-config='{"enable_multithread_load": true, "num_threads": 128}' \
    --quantization ascend \
    --port $VLLM_PORT \
    --block-size 128 \
    --speculative-config '{"num_speculative_tokens": 1,"method": "mtp","enforce_eager": true}' \
    --compilation-config '{"cudagraph_mode": "FULL_DECODE_ONLY"}' \
    --async-scheduling \
    --additional-config '{"ascend_compilation_config":{"enable_npugraph_ex":true,"enable_static_kernel":false},"enable_cpu_binding": true,"enable_dsa_cp": true,"multistream_overlap_shared_expert":true}' \
    "${KV_ARGS[@]}"
