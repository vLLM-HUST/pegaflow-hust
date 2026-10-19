from pathlib import Path
import runpy
import pegaflow
root = Path(__file__).resolve().parent.parent
pegaflow.__path__.insert(0,str(root/"overlay/pegaflow"))
runpy.run_path(str(root/"code/shared_pair.py"),run_name="__main__")
