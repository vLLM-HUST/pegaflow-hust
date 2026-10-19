#!/usr/bin/env bash
set -euo pipefail
task_role=${1:?P or D}
case "$task_role" in P|D) ;; *) exit 2 ;; esac
task_root=/root/issue23-shared-strategies/20261016-source02
task_output="$task_root/$task_role-native-pending-three-rates-01"
test ! -e "$task_output"
test ! -e "$task_output-launcher.log"
export ISSUE23_PIPELINE_REPLAY=1 ISSUE23_JOINT_ROOT="$task_root"
export ISSUE23_ROLE="$task_role"
export ISSUE23_JOINT_PYTHON=/root/issue23-tools/pegaflow-joint-pipeline-01/.venv/bin/python
cd "$task_root"
sha256sum -c source.sha256
bash code/env.sh code/preflight.py > "$task_output-preflight.json"
case "$task_role" in
 D) task_script=confirm_node.py ;;
 P) /usr/local/python3.12.13/bin/python3 code/wait_d_ready.py \
    --url http://10.17.119.71:18390/ --timeout 120 --output "$task_output-prelaunch-D-ready.json"
    task_script=shared_bootstrap.py ;;
esac
setsid /usr/local/python3.12.13/bin/python3 code/supervise.py \
 --output "$task_output" --timeout 21600 --memory-stop-gib 56 \
 --job-kind formal_native_vs_pending_three_rates_all_costs \
 -- bash code/env.sh "code/$task_script" --output "$task_output" \
 > "$task_output-launcher.log" 2>&1 < /dev/null &
task_pid=$!
sleep 1
ps -o pid,pgid,sid,args -p "$task_pid"
