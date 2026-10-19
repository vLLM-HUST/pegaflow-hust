"""Frozen independent complete-policy runtime; reuse bounded transport helpers."""

from pathlib import Path
import os
from legacy_common import *  # noqa: F403

ROOT = Path(os.environ.get("ISSUE23_JOINT_ROOT", "/root/issue23-joint/20261014"))
PHASES = ("baseline", "treatment") if os.environ.get("ISSUE23_PIPELINE_REPLAY") == "1" else PHASES
PYTHON = Path(os.environ.get("ISSUE23_JOINT_PYTHON", "/root/issue23-tools/pegaflow-joint-01/.venv/bin/python"))

