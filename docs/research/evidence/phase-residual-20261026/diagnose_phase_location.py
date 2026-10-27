"""Paired native-policy mechanism diagnostic; fixed-zero control, not net gains."""
import hashlib
import json
from pathlib import Path
import sys
import time

root=Path('/root/issue23-phase-residual-source-01')
out=Path('/root/issue23-phase-location-diagnostic-01')
out.mkdir(exist_ok=False)
sys.path[:0]=[str(root/'python'),str(root/'experiments/issue23_strategies')]
import simulate_continuous_batch as sim
from control_work_search import ControlWorkSimulation
time.perf_counter=lambda:0.0
inputs=Path('/root/issue23-batch-inputs-20261026/experiments/issue23_handoff/simulation-inputs.json')
seed=Path('/root/issue23-batch-inputs-20261026/experiments/issue23_strategies/runtime/calibration-seed')
data=json.loads(inputs.read_bytes()); values=dict(data['costs'])
values['service']=tuple(sim.parent.core.ObservedLatencyModel(**r) for r in values['service'])
values['decode_seconds']=tuple(values['decode_seconds'])
cases=[]
for index,rate in enumerate((.24516170769071072,.49032341538142143,.9806468307628429),1):
    arms={}
    for label,phase in [('aggregate',False),('joint_phase',True)]:
        model=ControlWorkSimulation(data['rows'],data['initial_inventory'],sim.Costs(**values),seed,
            data['control_cost_rows'],rate,'native',True,batch_config=sim.BatchConfig(),
            phase_residual_world=phase)
        arms[label]=model.run()
        (out/f'case-{index:02d}-{label}.json').write_text(json.dumps(arms[label],indent=2))
    old,new=arms.values()
    oldrows={r['id']:r for r in old['per_request']}; newrows={r['id']:r for r in new['per_request']}
    assert set(oldrows)==set(newrows)
    assert all((oldrows[k]['p'],oldrows[k]['d'])==(newrows[k]['p'],newrows[k]['d']) for k in oldrows)
    oldstages={(r['id'],r['node']):r for r in old['stages']}
    same=sum(oldstages[r['id'],r['node']]['prefix']==r['prefix'] for r in new['stages'])
    cases.append(dict(rate=rate,aggregate_p99=old['TTFT']['p99'],joint_p99=new['TTFT']['p99'],
        aggregate_phase=old['makespan'],joint_phase=new['makespan'],
        identical_routes=True,same_stage_prefix_coordinates=same,total_stages=190,
        all_control_CPU_zero=all(a['planning_wall']==a['boundary_CPU_wall']==a['iteration_scheduler_wall_charged']==0 for a in arms.values())))
report=dict(scope='Mechanism diagnostic: same native policy, source and fixed zero control; not charged performance or a measured hardware effect',
    input_sha256=hashlib.sha256(inputs.read_bytes()).hexdigest(),
    source_sha256='f7a70b711424b4583edcda267c98048f68c2c6ad972e9d0b908b07814934a376',cases=cases)
(out/'summary.json').write_text(json.dumps(report,indent=2))
(out/'diagnose_phase_location.py').write_bytes(Path(__file__).read_bytes())
print(json.dumps(report,indent=2))
