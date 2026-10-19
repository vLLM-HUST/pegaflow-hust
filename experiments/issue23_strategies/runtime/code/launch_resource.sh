#!/usr/bin/env bash
set -euo pipefail
task_role=${1:?P or D}
task_case=${2:?150 or 300 or 600}
case "$task_role" in P|D) ;; *) exit 2 ;; esac
case "$task_case" in 150|300|600) ;; *) exit 2 ;; esac
task_root=/root/issue23-resource-dag/20261015
task_output="$task_root/$task_role-formal-resource-DAG-poisson-$task_case-01"
test ! -e "$task_output"
test ! -e "$task_output-launcher.log"
export ISSUE23_PIPELINE_REPLAY=1 ISSUE23_JOINT_ROOT="$task_root"
export ISSUE23_JOINT_PYTHON=/root/issue23-tools/pegaflow-joint-pipeline-01/.venv/bin/python
cd "$task_root"
sha256sum -c source.sha256
bash "$task_root/code/env.sh" "$task_root/code/preflight.py" > "$task_output-preflight.json"
case "$task_role" in
 D) task_script=node.py; task_extra=() ;;
 P) /usr/local/python3.12.13/bin/python3 "$task_root/code/wait_d_ready.py" \
     --url http://10.17.119.71:18390/ --timeout 120 --output "$task_output-prelaunch-D-ready.json"
    task_script=bootstrap_resource.py
    task_extra=(--pipeline-reference /root/issue23-joint/20261014/P-formal-joint-ab-02 \
                --operation-seed "$task_root/calibration-seed" \
                --arrival-manifest "$task_root/arrival-suite-poisson$task_case.json" \
                --arrival-condition "poisson-$task_case" --event-queue-policy) ;;
esac
setsid /usr/local/python3.12.13/bin/python3 "$task_root/code/supervise.py" \
 --output "$task_output" --timeout 10800 --memory-stop-gib 56 \
 --job-kind resource_DAG_exposed_causal_replay_formal_AB \
 -- bash "$task_root/code/env.sh" "$task_root/code/$task_script" --output "$task_output" "${task_extra[@]}" \
 > "$task_output-launcher.log" 2>&1 < /dev/null &
task_pid=$!
sleep 1
ps -o pid,pgid,sid,args -p "$task_pid"
