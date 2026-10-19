#!/usr/bin/env bash
set -euo pipefail
task_role=${1:?P or D}
case "$task_role" in P|D) ;; *) exit 2 ;; esac
task_root=/root/issue23-phase-release/20261016-source01
task_output="$task_root/$task_role-phase-release-parent-new-poisson300-01"
test ! -e "$task_output"
test ! -e "$task_output-launcher.log"
export ISSUE23_PIPELINE_REPLAY=1 ISSUE23_JOINT_ROOT="$task_root"
export ISSUE23_JOINT_PYTHON=/root/issue23-tools/pegaflow-joint-pipeline-01/.venv/bin/python
cd "$task_root"
sha256sum -c source.sha256
bash "$task_root/code/env.sh" "$task_root/code/preflight.py" > "$task_output-preflight.json"
case "$task_role" in
 D) task_script=confirm_node.py; task_extra=() ;;
 P) /usr/local/python3.12.13/bin/python3 "$task_root/code/wait_d_ready.py" \
     --url http://10.17.119.71:18390/ --timeout 120 --output "$task_output-prelaunch-D-ready.json"
    task_script=confirm_bootstrap.py
    task_extra=(--pipeline-reference /root/issue23-joint/20261014/P-formal-joint-ab-02 \
                --candidate-protocol "$task_root/candidate-protocol.json" \
                --operation-seed "$task_root/calibration-seed" \
                --arrival-manifest "$task_root/arrival-suite-poisson300.json") ;;
esac
setsid /usr/local/python3.12.13/bin/python3 "$task_root/code/supervise.py" \
 --output "$task_output" --timeout 10800 --memory-stop-gib 56 \
 --job-kind formal_B_parent_new_B_phase_release_all_costs \
 -- bash "$task_root/code/env.sh" "$task_root/code/$task_script" --output "$task_output" "${task_extra[@]}" \
 > "$task_output-launcher.log" 2>&1 < /dev/null &
task_pid=$!
sleep 1
ps -o pid,pgid,sid,args -p "$task_pid"
