import json,sys,statistics
from pathlib import Path
p=Path('C:/Users/Lenovo/Desktop/issue23/cache-profile-20261026')
sys.path.insert(0,str(p.parent/'.pcie-20261026/experiments/issue23_strategies'))
from analyze_bounded_behavior import per_request
report={'scope':'Archived CPU mechanisms, not pure queue attribution or counterfactual strategy benefit','cases':[]}
root=p/'result-completed-phase-02/issue23-completed-phase-02'
for i in (1,2,3):
 c=json.loads((root/f'case-{i:02d}.json').read_bytes());entry={'rate':c['rate'],'arms':{}}
 maps={name:per_request(c['arms'][name]) for name in ('reference','ours')}
 for name in maps:
  arm=c['arms'][name];rows=maps[name];worst=max(rows,key=lambda k:rows[k]['TTFT'])
  decision=next(d for d in arm['decisions'] if d['id']==worst)
  overlaps=[]
  for d in arm['decisions']:
   found=[dict(node=e['node'],kind=e['kind'],age=d['origin']-e['start'],remaining_trace=e['end']-d['origin'],owned=[x[0] for x in e['pieces']]) for e in arm['batch_iterations'] if e['start']<d['origin']<e['end'] and e['kind'] in ('restore','save')]
   if found:overlaps.append(dict(key=d['id'],origin=d['origin'],phases=found))
  entry['arms'][name]=dict(worst=worst,path=rows[worst],initial=decision['initial_forecast'],same_request_other=maps['ours' if name=='reference' else 'reference'][worst],inflight_exclusive_at_route_origin=overlaps,CPU_total=arm['planning_wall']+arm['boundary_CPU_wall'],model_CPU=arm['fit_CPU_wall'])
 entry['largest_regressions']=[dict(key=k,delta=maps['ours'][k]['TTFT']-maps['reference'][k]['TTFT'],before_delta=maps['ours'][k]['before_P']-maps['reference'][k]['before_P'],P_delta=maps['ours'][k]['P_API']-maps['reference'][k]['P_API'],between_delta=maps['ours'][k]['P_to_D']-maps['reference'][k]['P_to_D'],Dfirst_delta=maps['ours'][k]['D_API_to_first']-maps['reference'][k]['D_API_to_first']) for k in sorted(maps['ours'],key=lambda k:maps['ours'][k]['TTFT']-maps['reference'][k]['TTFT'],reverse=True)[:5]]
 report['cases'].append(entry)
high=json.loads((root/'case-03.json').read_bytes())
report['high_same_cold_request']={'key':'git-multibranch:11','arms':{}}
for name in ('reference','ours'):
 arm=high['arms'][name];key='git-multibranch:11'
 stage=next(s for s in arm['stages'] if s['id']==key and s['node']>=2)
 start=stage['completed']-stage['full'];first=start+stage['first'];overlap={}
 for e in arm['batch_iterations']:
  if e['node']!=stage['node']:continue
  seconds=max(0,min(first,e['end'])-max(start,e['start']))
  if seconds:
   kind=e['kind']+(' containing_request' if any(x[0]==key for x in e['pieces']) else ' other')
   overlap[kind]=overlap.get(kind,0)+seconds
 report['high_same_cold_request']['arms'][name]={'stage':stage,'observed_interval_overlaps':overlap,'mixed_compute_interval_is_not_isolated_own_compute':True}
(p/'completed-phase-failure-paths-02.json').write_text(json.dumps(report,indent=2))
for c in report['cases']:
 print('RATE',c['rate'])
 for name,a in c['arms'].items():print(name,'worst',a['worst'],'path',a['path'],'CPU',a['CPU_total'],'phase overlaps',len(a['inflight_exclusive_at_route_origin']))
 print('largest',c['largest_regressions'][:2])
