#!/usr/bin/env bash
set -euo pipefail
task_role=${1:?P or D}
task_condition=${2:?Frozen arrival condition}
case "$task_condition" in
 poisson-050|poisson-075|poisson-100|gamma-cv2-100) ;;
 *) exit 2 ;;
esac
task_root=/root/issue23-arrivals/20261014
task_output="$task_root/$task_role-formal-arrival-ab-$task_condition-01"
test ! -e "$task_output"
test ! -e "$task_output-launcher.log"
/usr/local/python3.12.13/bin/python3 -c 'import json,sys; from pathlib import Path; p=Path(sys.argv[1]); r=json.loads((p/"result.json").read_text()); assert r["exit_code"] == 0 and r["stop_reason"] is None' \
 "/root/issue23-pipeline/20261014/$task_role-formal-pipeline-ab-03"
export ISSUE23_PIPELINE_REPLAY=1 ISSUE23_JOINT_ROOT="$task_root"
export ISSUE23_JOINT_PYTHON=/root/issue23-tools/pegaflow-joint-pipeline-01/.venv/bin/python
/usr/local/python3.12.13/bin/python3 -c 'import hashlib,sys; from pathlib import Path; assert hashlib.sha256(Path(sys.argv[1]).read_bytes()).hexdigest() == "07f4970713d8119a72ae29873216f19ab60724a2f48040c63cf2dc91f751c10a"' "$task_root/arrival-suite-02.json"
/usr/local/python3.12.13/bin/python3 -c 'import sys; sys.path.insert(0,sys.argv[1]); from common import assert_port_free,P_IP,D_IP; role=sys.argv[2]; assert_port_free(P_IP if role == "P" else D_IP,(50260,19260,50300,50301,18300,18301) if role == "P" else (18390,50300,50301,18300,18301))' "$task_root/code" "$task_role"
case "$task_role" in
 P) task_script=run.py; task_extra=(--pipeline-reference /root/issue23-joint/20261014/P-formal-joint-ab-02 --arrival-manifest "$task_root/arrival-suite-02.json" --arrival-condition "$task_condition") ;;
 D) task_script=node.py; task_extra=() ;;
 *) exit 2 ;;
esac
setsid /usr/local/python3.12.13/bin/python3 "$task_root/code/supervise.py" \
 --output "$task_output" --timeout 10800 --memory-stop-gib 56 \
 --job-kind causal_synthetic_arrival_independent_PD_formal_ab \
 -- bash "$task_root/code/env.sh" "$task_root/code/$task_script" --output "$task_output" "${task_extra[@]}" \
 > "$task_output-launcher.log" 2>&1 < /dev/null &
task_pid=$!
sleep 1
ps -o pid,pgid,sid,args -p "$task_pid"
