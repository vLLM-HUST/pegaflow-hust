import json
import sys
from pathlib import Path
from dataclasses import asdict
art=Path(__file__).resolve().parent
repo=art.parent/'.pcie-20261026'
sys.path[:0]=[str(repo/'experiments/issue23_strategies'),str(repo/'python')]
import simulate_continuous_batch as sim
from ready_prefill_search import ReadyPrefillSimulation
from staged_save_model import StagedSaveSimulationMixin, SaveAllocationError
from pegaflow.strategies.memory_budget import Ascend910B2Budget
root=art.parent/'benchmark-collection-20261026'
data=json.loads((root/'experiments/issue23_handoff/simulation-inputs.json').read_bytes())
v=dict(data['costs']);v['service']=tuple(sim.parent.core.ObservedLatencyModel(**r) for r in v['service']);v['decode_seconds']=tuple(v['decode_seconds'])
cls=type('StagedDiagnostic',(StagedSaveSimulationMixin,ReadyPrefillSimulation),{})
m=cls(data['rows'],data['initial_inventory'],sim.Costs(**v),root/'experiments/issue23_strategies/runtime/calibration-seed',data['control_cost_rows'],.49032341538142143,'native',True,batch_config=sim.BatchConfig(max_sequences=4,bytes_per_token=v['bytes_per_token'],kv_bytes=Ascend910B2Budget().kv_bytes),phase_residual_world=True)
try:
    m.run()
except SaveAllocationError as e:
    event=m.actions[-1]
    node=event['node']
    units=m.layout.units[node]
    result=dict(diagnostic_not_performance=True,error=repr(e),at=m.now,save_failure=event,
                used=m.layout.usage(node),capacity=m.layout.capacities[node],hidden=m.layout.hidden[node],
                virtual_bytes=sum(u.charged for u in units if u.virtual),protected_bytes=sum(u.charged for u in units if u.protected),
                units=[dict(identity=u.identity,charged=u.charged,virtual=u.virtual,protected=u.protected,keys=[k.hex() for k in u.keys]) for u in units],
                completed=len(m.finished))
    (art/'staged-save-failure-diagnosis-03.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
    print(json.dumps({k:v for k,v in result.items() if k!='units'}))
else:
    result=dict(diagnostic_not_performance=True,original_failure_absent=True,completed=len(m.finished),copy_publications=sum(e['kind']=='copy_publication' for e in m.actions),save_publications=sum(e['kind']=='save_publication' for e in m.actions)); (art/'staged-save-failure-diagnosis-03.json').write_text(json.dumps(result,indent=2),encoding='utf-8'); print(json.dumps(result))
