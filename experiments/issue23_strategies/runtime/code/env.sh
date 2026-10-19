#!/usr/bin/env bash
set -euo pipefail
unset LD_PRELOAD PYTHONPATH
set +u
source /usr/local/Ascend/ascend-toolkit/set_env.sh
source /usr/local/Ascend/nnal/atb/set_env.sh
set -u
export PYTHONHASHSEED=20261014 VLLM_WORKER_MULTIPROC_METHOD=spawn
export HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1 TOKENIZERS_PARALLELISM=false
export OMP_NUM_THREADS=4 XDG_CACHE_HOME=/root/.cache VLLM_CACHE_ROOT=/root/.cache/vllm
export TMPDIR="${ISSUE23_JOINT_ROOT:-/root/issue23-joint/20261014}/tmp"
mkdir -p "$TMPDIR"
exec "${ISSUE23_JOINT_PYTHON:-/root/issue23-tools/pegaflow-joint-01/.venv/bin/python}" "$@"
