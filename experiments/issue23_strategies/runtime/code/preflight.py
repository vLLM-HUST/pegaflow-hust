"""Record allocation/free capacity and refuse duplicate owned inference jobs."""

import json
import os
import subprocess
import ipaddress
import shutil
from pathlib import Path

os.environ["ASCEND_RT_VISIBLE_DEVICES"] = "0,1"
import torch
import torch_npu  # noqa: F401

if torch.npu.device_count() != 2:
    raise RuntimeError("Exactly two allocated container logical NPUs required")
devices = [{"logical": n, "free_bytes": torch.npu.mem_get_info(n)[0],
            "total_bytes": torch.npu.mem_get_info(n)[1]} for n in range(2)]
if any(value["free_bytes"] < 0.35 * value["total_bytes"] for value in devices):
    raise RuntimeError("Actual free capacity is below the frozen single-replica budget")
if not Path("/data/shared_models/Qwen2.5-7B-Instruct/config.json").exists():
    raise RuntimeError("Shared model is missing")
mapping = subprocess.run(["npu-smi", "info", "-m"], check=True, capture_output=True, text=True).stdout
state = subprocess.run(["npu-smi", "info"], check=True, capture_output=True, text=True).stdout
processes = subprocess.run(["ps", "-eo", "pid,ppid,args"], check=True, capture_output=True, text=True).stdout
if any("code/worker.py" in line and "issue23-" in line for line in processes.splitlines()):
    raise RuntimeError("Existing owned model process; do not duplicate")
gid = Path("/sys/class/infiniband/mlx5_2/ports/1/gids/5").read_text().strip()
role = os.environ.get("ISSUE23_ROLE")
expected = {"P":"10.17.119.1","D":"10.17.119.71"}.get(role)
actual = ipaddress.IPv6Address(gid).ipv4_mapped
if expected is None or str(actual)!=expected:
    raise RuntimeError("Allocation/IP changed; inspect the new GID mapping before deployment")
if shutil.disk_usage("/root").free < 4*1024**3:
    raise RuntimeError("Insufficient root safety space for six-arm evidence; do not delete old data blindly")
print(json.dumps({"devices": devices, "mapping": mapping, "npu_smi": state,
                  "roce_gid5": gid,"actual_IP":str(actual),"role":role,
                  "root_free_bytes": __import__("shutil").disk_usage("/root").free,
                  "two_instances_per_host_tp1": True}, indent=2))
