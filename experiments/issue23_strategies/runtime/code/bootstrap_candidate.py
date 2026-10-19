"""Load only the new pure-Python controller policy; retain serving installation."""

import runpy
import sys
from pathlib import Path

import pegaflow

root = Path(__file__).resolve().parent.parent
pegaflow.__path__.insert(0, str(root / "overlay/pegaflow"))
runpy.run_path(str(root / "code/run.py"), run_name="__main__")
