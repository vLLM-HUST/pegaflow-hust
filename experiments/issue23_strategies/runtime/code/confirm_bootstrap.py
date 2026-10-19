"""Apply the frozen controller overlay only, never the serving workers."""

from pathlib import Path
import runpy

import pegaflow

root = Path(__file__).resolve().parent.parent
pegaflow.__path__.insert(0, str(root / "overlay/pegaflow"))
runpy.run_path(str(root / "code/confirm_run.py"), run_name="__main__")
