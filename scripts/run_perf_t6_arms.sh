#!/bin/bash
# run_perf_t6_arms.sh — T6 三臂验证编排: native / isolated-share / always-share.
#
# 每臂: 起 vLLM (按臂注入 server 端口/池) -> 跑对应 workload -> 收命中指标。
# isolated-share 每域一个独立 pegaflow-server (物理池隔离), vLLM 重启换连。
#
# Preregistered (perf plan §T6):
#   - native:        无 PegaFlow, 前缀缓存关 -> 基线命中率 0
#   - isolated-share:每域独立 server+池 (chat -> 50081, agent -> 50082),
#                    域间零共享
#   - always-share:  单 server (50080) 双域共享 -> 跨域共享收益
#   - deliverable: always-share 命中率 - isolated-share 命中率 = 跨域共享收益
#
# 用法:
#   bash scripts/run_perf_t6_arms.sh [--codex-json /data/codex_swebenchpro.json]
#                                     [--arms native,isolated-share,always-share]
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)
SERVE="$(cd "$(dirname "$0")" && pwd)/t6-v4-serve.sh"
CONDA_ENV="${CONDA_ENV:-deepseek-v4-deploy}"
# workload 需要 conda 环境的 python (serve 脚本内部自行 activate)
source ${CONDA_ROOT:-/root/miniconda3}/bin/activate $CONDA_ENV
WORKLOAD="python $(cd "$(dirname "$0")" && pwd)/run_perf_t6_workload.py"
CODEX_JSON="${CODEX_JSON:-}"
ARMS="${ARMS:-native,isolated-share,always-share}"
VLLM_PORT=8900

cleanup() {
  pkill -9 -f "vllm serve" 2>/dev/null || true
  pkill -9 -f "VLLM::" 2>/dev/null || true
  pkill -9 -f "pegaflow-server --addr" 2>/dev/null || true
}
trap cleanup EXIT

wait_startup() {  # $1 = log, $2 = serve script pid
  for _ in $(seq 1 150); do
    grep -q "Application startup complete" "$1" 2>/dev/null && return 0
    # 存活判断看 serve 脚本 PID (它先做显存探测 ~30-60s 才 spawn vllm),
    # 不能 pgrep "vllm serve" — 探测期会误判死亡。
    kill -0 "$2" 2>/dev/null || { echo "  [FAIL] serve script died"; tail -8 "$1"; return 1; }
    sleep 5
  done
  echo "  [FAIL] startup timeout"; return 1
}

start_arm() {  # $1 = arm, $2 = log, $3 = extra env (PEGAFLOW_PORT=...)
  local attempt
  for attempt in 1 2 3; do
    cleanup
    sleep 5
    echo "== 起臂: $1 (log $2) 尝试 $attempt =="
    if [ "$1" = "native" ]; then
      env $3 GMU=${GMU:-0.85} bash "$SERVE" --no-pegaflow > "$2" 2>&1 &
    else
      env $3 GMU=${GMU:-0.85} bash "$SERVE" > "$2" 2>&1 &
    fi
    local pid=$!
    if wait_startup "$2" "$pid"; then
      echo "  [OK] startup complete"
      return 0
    fi
    # 共享机器显存波动: 失败等 60s 重试 (gmu 0.85 = 51.8GiB 需求)
    [ "$attempt" -lt 3 ] && { echo "  [RETRY] 60s 后重试"; sleep 60; }
  done
  return 1
}

summary() {  # $1 = arm, $2 = grep pattern, $3 = log
  local line
  line=$(grep "汇总\|gate\|命中率" "$2" 2>/dev/null | tail -3)
  echo "### $1: $line"
}

rm -f /tmp/t6-arms-*.log

if [[ "$ARMS" == *native* ]]; then
  start_arm native /tmp/t6-arms-native.log ""
  $WORKLOAD --arm native --domains all --log /dev/null --port $VLLM_PORT \
    --codex-json "$CODEX_JSON" 2>&1 | tee /tmp/t6-arms-native-summary.log
fi

if [[ "$ARMS" == *isolated-share* ]]; then
  # chat 域 -> 独立池 50081
  start_arm isolated-share-chat /tmp/t6-arms-iso-chat.log "PEGAFLOW_PORT=50081"
  $WORKLOAD --arm isolated-share --domains chat --log /tmp/t6-arms-iso-chat.log \
    --port $VLLM_PORT 2>&1 | tee /tmp/t6-arms-iso-chat-summary.log
  # agent 域 -> 独立池 50082 (vLLM 重启换连)
  start_arm isolated-share-agent /tmp/t6-arms-iso-agent.log "PEGAFLOW_PORT=50082"
  $WORKLOAD --arm isolated-share --domains agent --log /tmp/t6-arms-iso-agent.log \
    --port $VLLM_PORT --codex-json "$CODEX_JSON" 2>&1 | tee /tmp/t6-arms-iso-agent-summary.log
fi

if [[ "$ARMS" == *always-share* ]]; then
  # 单池 50080, 双域
  start_arm always-share /tmp/t6-arms-share.log "PEGAFLOW_PORT=50080"
  $WORKLOAD --arm always-share --domains all --log /tmp/t6-arms-share.log \
    --port $VLLM_PORT --codex-json "$CODEX_JSON" 2>&1 | tee /tmp/t6-arms-share-summary.log
fi

echo
echo "===== T6 三臂汇总 ====="
for s in /tmp/t6-arms-*-summary.log; do
  [ -f "$s" ] && { echo "--- $(basename $s) ---"; grep "汇总\|gate" "$s" | tail -3; }
done
