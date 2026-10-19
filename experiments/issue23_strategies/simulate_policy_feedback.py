"""Causal cache/queue feedback screening, not hardware or output validation.

The world uses isolated32 stage fits/residuals; no future test service label,
route or hit is supplied to a policy. Known prompt hashes come from inputs.
RPC overhead is an aggregate of previously completed runs, not per-task labels.
Native default source ordering and incremental cache publication are approximated
and must be checked against real native logs before performance claims.
"""
from __future__ import annotations

import argparse
from collections import deque
from dataclasses import replace
import hashlib
import heapq
import itertools
import json
import math
from pathlib import Path
import sys
import time

import pegaflow
BASE = Path(__file__).resolve().parent
pegaflow.__path__.insert(0, str(BASE / "runtime/overlay/pegaflow"))
pegaflow.__path__.append(str(BASE.parents[1] / "python/pegaflow"))
sys.path.insert(0, str(BASE / "runtime/code"))
import run as core
from pegaflow.joint_policy import Costs, Demand, Layout, View, copy_package
from pegaflow.resource_cache_profile import CacheForecast
from pegaflow.strategies import Candidate, Request, Snapshot, Worker, create_strategy
from pegaflow.strategies.resource_forecast import ResourceForecaster, candidate_work
from pegaflow.strategies.resource_schedule import Operation
from arrivals import offsets
from analyze_phase_feedback import quantiles
from pegaflow.phase_release_calendar import PhaseReleaseQueues
from pegaflow.strategies.live_adapter import controlled_route
from pegaflow.resource_queue import direction

RATES = (.24516170769071072, .49032341538142143, .9806468307628429)


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))

class Simulation:
    def __init__(self, rows, initial, costs, seed, timing_rows, rate, mode, residual_world):
        self.rows, self.costs, self.mode = rows, costs, mode
        seed_began = time.perf_counter()
        self.models = core.seed_operations(costs, seed, "phase")
        seed_CPU = time.perf_counter()-seed_began
        world_models = core.seed_operations(costs, seed, "phase")
        self.world = world_models.freeze(0.)
        self.world_seed = world_models.phase_calibration
        self.residual_world = residual_world
        self.policy_queues = PhaseReleaseQueues(self.models,512,memoize=True,index_projection=True,predict_control=False)
        self.policy_queues.started = 0.
        self.solver_uses_same_live_adapter = True
        self.layout = Layout.current(tuple(View.parse(n, v) for n, v in enumerate(initial)))
        self.original_layout = self.layout
        self.times = offsets(len(rows), rate, 1., 20261014)
        self.now, self.serial = (seed_CPU if mode!="native" else 0.), itertools.count()
        self.events, self.pending = [], deque()
        self.jobs, self.finished, self.sessions = {}, {}, {}
        self.queues = [deque() for _ in range(4)]
        self.active, self.held, self.pins = {}, {}, {}
        self.actions, self.api_records, self.planning, self.cpu_samples = [], [], [], []
        self.resource_events = []
        self.peak_usage = list(self.layout.usage_values)
        self.boundary_CPU = []
        self.fit_CPU = []
        self.active_sessions, self.model_latest = set(), 0.
        self.router_busy, self.sequence = False, 0
        self.world_kind = "isolated-calibration-residual" if residual_world else "isolated-calibration-mean"
        self.rpc_route = sum(max(0., r["route_seconds"] - r["route_cpu_seconds"])
                             for r in timing_rows) / len(timing_rows)
        self.rpc_boundary = sum(max(0., r["D_boundary_ready_seconds"] - r["D_boundary_start_seconds"]
                                   - r["D_boundary_CPU_seconds"]) for r in timing_rows) / len(timing_rows)
        for row, at in zip(rows, self.times, strict=True):
            self.event(at, "arrival", row["id"])
        self.rowmap = {r["id"]: r for r in rows}

    def event(self, at, kind, key, payload=None):
        heapq.heappush(self.events, (at, next(self.serial), kind, key, payload))

    def frozen(self):
        return self.models.freeze(self.now)

    def item(self, row):
        return Demand(row["id"], row["task"], row["tokens"], tuple(bytes.fromhex(h) for h in row["hashes"]))

    def workers(self, row, frozen):
        item = self.item(row)
        return tuple(Worker(n, "P" if n < 2 else "D", self.layout.prefix(n, item.hashes)*128,
                            sum(self.jobs[k]["row"]["tokens"] for k in self.queues[n]),
                            int(n in self.active), len(self.queues[n]), 0., self.now)
                     for n in range(4))

    def candidates(self, row, frozen):
        item = self.item(row)
        prefix = [self.layout.prefix(n, item.hashes) for n in range(4)]
        control = self.rpc_route + (sum(self.cpu_samples)/len(self.cpu_samples) if self.cpu_samples else 0.)
        for p, d, source in itertools.product((0, 1), (2, 3), range(4)):
            for pb in dict.fromkeys((prefix[p], prefix[source], 0)):
                if pb > prefix[source]:
                    continue
                try:
                    history = (copy_package(self.layout, source, p, item.hashes[:pb]).operations[-1].payload_bytes
                               if source != p and pb > prefix[p] else 0)
                except ValueError:
                    continue
                for db in dict.fromkeys((prefix[d], max(prefix[d], prefix[p], pb), 0)):
                    try:
                        handoff = (copy_package(self.layout, p if prefix[p] >= db else source, d,
                                                item.hashes[:db]).operations[-1].payload_bytes
                                   if db > prefix[d] else 0)
                    except ValueError:
                        continue
                    pf = frozen.compute(p, item.tokens, pb*128)
                    df = frozen.compute(d, item.tokens, db*128)
                    yield Candidate(p, d, source, pb*128, db*128, pf, pf, df,
                                    df + frozen.decode_tail(d, item.tokens),
                                    frozen.copy_seconds(source, p, history),
                                    frozen.copy_seconds(p, d, handoff), control+self.rpc_boundary)

    def choose(self, row, ready):
        began = time.perf_counter()
        frozen = self.frozen()
        observed = frozenset(k for k, j in self.jobs.items() if j["work"].actual_first is not None)
        pending = frozenset(k for k in self.jobs if k not in observed) | {row["id"]}
        request = Request(row["id"], row["task"], row["tokens"], ready, self.sequence)
        plans = ()
        snap = Snapshot(request, self.now, self.workers(row, frozen), plans, pending, observed)
        live_route = None
        if self.mode == "native":
            decision = create_strategy("vllm-pegaflow-native").choose(snap)
            pf = frozen.compute(decision.p, row["tokens"], 0)
            df = frozen.compute(decision.d, row["tokens"], 0)
            plan = Candidate(decision.p, decision.d, decision.p, 0, 0, pf, pf, df,
                             df+frozen.decode_tail(decision.d, row["tokens"]))
        else:
            from pegaflow.joint_policy import Calendar
            demand = replace(self.item(row),arrival=self.now)
            live_route,tried,_ = controlled_route(demand,self.layout,frozen,self.policy_queues,
                                                  Calendar(),ready,self.now)
            pf = frozen.compute(live_route.p,row["tokens"],live_route.p_blocks*128)
            df = frozen.compute(live_route.d,row["tokens"],live_route.d_blocks*128)
            plan = Candidate(live_route.p,live_route.d,live_route.source,
                             live_route.p_blocks*128,live_route.d_blocks*128,pf,pf,df,
                             df+frozen.decode_tail(live_route.d,row["tokens"]))
        cpu = time.perf_counter()-began
        self.planning.append({"id": row["id"], "CPU_wall": cpu,
                              "model_latest": frozen.latest_completed_seconds, "origin": self.now,
                              "candidates": 0 if self.mode=="native" else tried})
        return plan, candidate_work(snap, plan), cpu,live_route

    def route(self):
        if self.router_busy:
            return
        key = next((k for k in self.pending if self.rowmap[k]["task"] not in self.active_sessions), None)
        if key is None:
            return
        self.pending.remove(key)
        row = self.rowmap[key]
        offered = self.times[self.rows.index(row)]
        ready = max(offered, self.sessions.get(row["task"], 0.))
        plan, work, cpu, live_route = self.choose(row, ready)
        self.sequence += 1
        self.jobs[key] = dict(row=row, plan=plan, work=replace(work, completed_operations=frozenset({"control"})),
                              live_route=live_route,
                              ready=ready, offered=offered, phase=0, steps=deque(),
                              first=None, stage_timing={}, pred_CPU=cpu)
        self.active_sessions.add(row["task"])
        self.router_busy = True
        self.event(self.now+cpu+(self.rpc_route if self.mode != "native" else 0.), "routed", key)

    def pin(self, node, hashes, sign):
        identities = [u.identity for u in self.layout.units[node] if u.keys.intersection(hashes)]
        for identity in identities:
            key = node, identity
            self.pins[key] = self.pins.get(key, 0) + sign
            if self.pins[key] < 0:
                raise ValueError("Simulated source lease double release")
        nodes = list(self.layout.units)
        nodes[node] = tuple(replace(u, protected=self.pins.get((node, u.identity), 0)>0)
                            for u in nodes[node])
        self.layout = replace(self.layout, units=tuple(nodes))

    def native_reclaim(self, node, amount):
        if self.layout.usage(node) + amount <= self.layout.capacities[node]:
            return
        eligible = [u for u in self.layout.units[node] if not u.protected and not u.virtual]
        victims, logical = [], 0
        for unit in eligible:
            victims.append(unit.identity)
            logical += len(unit.keys)
            if logical >= 512:
                break
        self.layout = self.layout.remove(node, victims)
        self.actions.append(dict(kind="native_reclaim", node=node, logical=logical, at=self.now))

    def stage(self, key, node):
        job, frozen = self.jobs[key], self.frozen()
        item, plan = self.item(job["row"]), job["plan"]
        role = "P" if node < 2 else "D"
        local = self.layout.prefix(node, item.hashes)
        if self.mode == "native":
            source = min(range(4), key=lambda n: (-self.layout.prefix(n, item.hashes), n != node, n))
            count = self.layout.prefix(source, item.hashes)
        elif node < 2:
            source = plan.source
            count = min(plan.p_prefix//128, max(local, self.layout.prefix(source, item.hashes)))
        else:
            source = plan.p
            limit = max(local, self.layout.prefix(source, item.hashes))
            options = []
            for count in dict.fromkeys((local, limit, 0)):
                missing = max(0, count-local)*128*self.costs.bytes_per_token
                try:
                    if missing:
                        copy_package(self.layout, source, node, item.hashes[:count])
                except ValueError:
                    continue
                options.append((frozen.compute(node,item.tokens,count*128)+frozen.copy_seconds(source,node,missing),count))
            count = min(options)[1]
            boundary_began = time.perf_counter()
            count,_ = self.policy_queues.decode_boundary(key,job["live_route"],self.layout,frozen,self.now)
            boundary_CPU = time.perf_counter()-boundary_began
            self.boundary_CPU.append(boundary_CPU)
            job["live_route"] = replace(job["live_route"],d_blocks=count)
        if self.mode != "native":
            self.policy_queues.revise(key,replace(job["live_route"],**{"p_blocks" if node<2 else "d_blocks":count}),self.layout,frozen)
        copy = None
        if source != node and count > local:
            try:
                copy = copy_package(self.layout, source, node, item.hashes[:count]).operations[-1]
            except ValueError:
                if self.mode == "native":
                    self.native_reclaim(node, (count-local)*128*self.costs.bytes_per_token)
                    try:
                        copy = copy_package(self.layout, source, node, item.hashes[:count]).operations[-1]
                    except ValueError:
                        count = min(local, self.layout.prefix(node,item.hashes))
                else:
                    count = min(count, local)
        steps = deque()
        if node >= 2 and self.mode != "native":
            steps.append(("D_control", self.rpc_boundary+boundary_CPU, (), None, None))
        if copy:
            duration = self.world.copy_seconds(source,node,copy.payload_bytes)
            resources = (f"gate{node}",) if self.mode == "native" else tuple(sorted({f"gate{source}",f"gate{node}"}))
            steps.append(("history" if node<2 else "handoff",duration,resources,
                          (source,node,item.hashes[:count],copy.payload_bytes),None))
        first, tail = self.physics(key,node,item.tokens,count*128)
        steps.append((role+"_api",first+tail,(f"gate{node}",),None,first if node>=2 else None))
        job["steps"], job["count"], job["node"] = steps,count,node
        pred_first = frozen.compute(node,item.tokens,count*128)
        pred_ops = []
        for name,duration,resources,copy_data,first_at in steps:
            if name.endswith("_api"):
                duration = pred_first + (frozen.decode_tail(node,item.tokens) if node>=2 else 0.)
                first_at = pred_first if node>=2 else None
            elif copy_data:
                duration = frozen.copy_seconds(copy_data[0],node,copy_data[3])
            pred_ops.append(Operation(name,duration,resources,first_at))
        job["work"] = replace(job["work"], **{"prefill" if node<2 else "decode": tuple(pred_ops),
            "p_started" if node<2 else "d_started": self.now})
        job["stage_timing"][role+"_start"] = self.now

    def physics(self,key,node,tokens,prefix):
        first,tail = self.world.compute(node,tokens,prefix),0.
        if node>=2:
            tail = self.world.decode_tail(node,tokens)
        rows = [(r,w) for n,r,w in self.world_seed if n==node and bool(r["num_cached_tokens"])==bool(prefix)]
        if self.residual_world and rows:
            index = int.from_bytes(hashlib.sha256((key+":"+str(node)).encode()).digest()[:8],"big")%len(rows)
            result,wire = rows[index]
            point = self.world.compute(node,result["prompt_tokens"],result["num_cached_tokens"])
            first = max(0., first+wire["first_seconds"]-point)
            if node>=2:
                point_tail = self.world.decode_tail(node,result["prompt_tokens"])
                tail = max(0.,tail+wire["complete_seconds"]-wire["first_seconds"]-point_tail)
        return first,tail

    def pump(self):
        changed = True
        while changed:
            changed = False
            for node in range(4):
                if node not in self.active and self.queues[node]:
                    key = self.queues[node].popleft()
                    self.active[node] = key
                    if self.mode != "native":
                        self.policy_queues.observe(key,node,"started",self.now)
                    self.stage(key,node)
                    changed = True
            for node,key in sorted(tuple(self.active.items())):
                job = self.jobs[key]
                if job.get("running") or not job["steps"]:
                    continue
                operation = job["steps"][0]
                name,duration,resources,copy_data,first = operation
                if copy_data and self.mode != "native":
                    gates = tuple(sorted({f"gate{copy_data[0]}",f"gate{node}"}))
                    if not job.get("copy_gate_hold"):
                        if any(r in self.held for r in gates):
                            continue
                        for resource in gates:
                            self.held[resource] = key,name
                        job["copy_gate_hold"] = gates,self.now
                        combined = (*gates,"link:"+direction(copy_data[0],node))
                        self.policy_queues.operation(key,name,"start",self.now,combined)
                    resources = ("link:"+direction(copy_data[0],node),)
                    operation = (name,duration,resources,copy_data,first)
                    job["steps"][0] = operation
                    if any(r in self.held for r in resources):
                        continue
                if any(r in self.held for r in resources):
                    continue
                for resource in resources:
                    self.held[resource] = key,name
                if copy_data:
                    self.pin(copy_data[0],set(copy_data[2]),1)
                else:
                    if name.endswith("_api"):
                        self.pin(node,set(self.item(job["row"]).hashes[:job["count"]]),1)
                job["running"] = operation
                self.resource_events.append(dict(request=key,node=node,operation=name,
                    start=self.now,end=self.now+duration,resources=resources,
                    first=self.now+first if first is not None else None))
                if self.mode != "native" and name!="D_control" and not copy_data:
                    if name.endswith("_api"):
                        self.policy_queues.skip_copy(key,"history" if node<2 else "handoff")
                    self.policy_queues.operation(key,name,"start",self.now,resources)
                job["work"] = replace(job["work"], active_operations=((name,self.now),))
                if first is not None:
                    self.event(self.now+first,"first",key)
                self.event(self.now+duration,"operation",key,operation)
                changed = True

    def complete_operation(self,key,operation):
        job = self.jobs[key]
        name,duration,resources,copy_data,first = operation
        node = job["node"]
        if self.mode != "native" and name!="D_control":
            self.policy_queues.operation(key,name,"finish",self.now)
        for resource in resources:
            if self.held.pop(resource) != (key,name):
                raise ValueError("Simulation gate ownership changed")
        if copy_data:
            source,destination,hashes,payload = copy_data
            hold = job.pop("copy_gate_hold",None)
            if hold is not None:
                gates,acquired = hold
                for resource in gates:
                    if self.held.pop(resource)!=(key,name):
                        raise ValueError("Copy source/destination software gate changed")
                link_wait = max(0.,self.now-duration-acquired)
                fit_began = time.perf_counter()
                self.models.observe_copy(source,destination,payload,self.now-acquired,link_wait,self.now)
                fit_CPU = time.perf_counter()-fit_began
                self.fit_CPU.append(fit_CPU)
                self.now += fit_CPU
                self.actions.append(dict(kind="software_copy_gate_hold",source=source,destination=destination,
                    acquired=acquired,released=self.now,link_wait=link_wait,not_RDMA_congestion_evidence=True))
            self.pin(source,set(hashes),-1)
            try:
                transition = copy_package(self.layout,source,destination,hashes)
            except ValueError:
                self.actions.append(dict(kind="copy_normal_defer",at=self.now,source=source,destination=destination))
                job["count"] = min(job["count"],self.layout.prefix(node,self.item(job["row"]).hashes))
                # Recompute both the actual and quoted unacquired API after failed publication.
                pfirst,ptail = self.physics(key,node,job["row"]["tokens"],job["count"]*128)
                api = job["steps"][-1]
                job["steps"][-1] = (api[0],pfirst+ptail,api[2],None,pfirst if node>=2 else None)
            else:
                self.layout = transition.layout
                self.actions.append(dict(kind="implicit_copy" if self.mode=="native" else "selected_copy",
                    at=self.now,source=source,destination=destination,payload=payload,seconds=duration))
        if name.endswith("_api"):
            item = self.item(job["row"])
            self.pin(node,set(item.hashes[:job["count"]]),-1)
            state = CacheForecast(self.layout,{key:item},{key:job["plan"]},self.world,512)
            state.save(node,item)
            self.layout = state.layout
            self.actions.extend(state.events)
            prefix = job["count"]*128
            actual_first = first if node>=2 else duration
            restore = min(actual_first,self.world.restore(node,prefix)) if prefix else 0.
            result = {"id":key,"prompt_tokens":item.tokens,"num_cached_tokens":prefix,
                "raw_metrics":{"queued_ts":0.,"scheduled_ts":restore,
                    "first_token_ts":actual_first,"last_token_ts":duration}}
            wire = {"first_seconds":actual_first,"complete_seconds":duration}
            if self.mode != "native":
                fit_began = time.perf_counter()
                self.models.observe(node,result,wire,self.now)
                fit_CPU = time.perf_counter()-fit_began
                self.fit_CPU.append(fit_CPU)
                self.now += fit_CPU
                self.policy_queues.observe(key,node,"finished",self.now)
            self.api_records.append(dict(id=key,node=node,prefix=prefix,tokens=item.tokens,first=actual_first,
                full=duration,restore_path_not_pure_H2D=restore,completed=self.now))
        job["running"] = None
        job["steps"].popleft()
        job["work"] = replace(job["work"], active_operations=(),
            completed_operations=job["work"].completed_operations|{name})
        if job["steps"]:
            return
        del self.active[node]
        if node<2:
            job["work"] = replace(job["work"],p_done=self.now,d_enqueued=self.now)
            self.queues[job["plan"].d].append(key)
            if self.mode != "native":
                self.policy_queues.observe(key,job["plan"].d,"enqueued",self.now)
        else:
            if job["first"] is None:
                raise ValueError("Full completion before observed first")
            self.finished[key] = dict(id=key,TTFT=job["first"]-job["ready"],
                nominal=job["first"]-job["offered"],dependency=job["ready"]-job["offered"],
                p=job["plan"].p,d=job["plan"].d,end=self.now)
            self.sessions[job["row"]["task"]] = self.now
            self.active_sessions.remove(job["row"]["task"])
            del self.jobs[key]

    def run(self):
        while self.events:
            event_time,_,kind,key,value = heapq.heappop(self.events)
            self.now = max(self.now,event_time)
            if kind=="arrival":
                self.pending.append(key)
            elif kind=="routed":
                self.router_busy = False
                job = self.jobs[key]
                self.cpu_samples.append(job["pred_CPU"])
                self.queues[job["plan"].p].append(key)
                if self.mode != "native":
                    self.policy_queues.observe(key,job["plan"].p,"enqueued",self.now)
            elif kind=="first":
                job = self.jobs[key]
                job["first"] = self.now
                if self.mode != "native":
                    self.policy_queues.observe(key,job["plan"].d,"first",self.now)
                job["work"] = replace(job["work"],actual_first=self.now)
            else:
                self.complete_operation(key,value)
            self.route()
            self.pump()
            self.peak_usage = [max(a,b) for a,b in zip(self.peak_usage,self.layout.usage_values)]
            if not self.layout.feasible():
                raise ValueError("Simulated physical capacity exceeded")
        if len(self.finished)!=len(self.rows) or self.jobs or self.pending or self.held:
            raise ValueError("Incomplete causal simulation")
        return {"policy":self.mode,"world":self.world_kind,"requests":len(self.finished),
            "same_routing_and_D_boundary_solver_as_live":True,
            "TTFT":quantiles([r["TTFT"] for r in self.finished.values()]),
            "nominal":quantiles([r["nominal"] for r in self.finished.values()]),"makespan":self.now,
            "planning_wall":sum(r["CPU_wall"] for r in self.planning),
            "boundary_CPU_wall":sum(self.boundary_CPU),"fit_CPU_wall":sum(self.fit_CPU),
            "fit_CPU_in_virtual_clock_conservative_controller_serialization":True,
            "model_seed_CPU_charged_only_to_our_runtime":True,
            "resource_events":self.resource_events,"peak_used_bytes":self.peak_usage,
            "point_costs_do_not_establish_tail_hardware_fidelity":True,
            "actual_source_destination_then_software_direction_lock_modeled":True,
            "route_RPC_mean":self.rpc_route,"boundary_RPC_mean":self.rpc_boundary,
            "all_routing_boundary_migration_in_simulated_clock":True,"copy_seconds":sum(r.get("seconds",0) for r in self.actions),
            "capacity_bytes":self.layout.capacities,"stages":self.api_records,"actions":self.actions,
            "decisions":self.planning,"per_request":list(self.finished.values()),
            "initial64_plus_completed_stages":len(self.models.records),
            "source_selection_and_publication_are_approximations":True,
            "slow_layout_policy":"off (preserved full parent remains available; no forced actions)"}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--parent",type=Path)
    parser.add_argument("--input-bundle",type=Path)
    parser.add_argument("--seed",type=Path,default=BASE/"runtime/calibration-seed")
    parser.add_argument("--output",type=Path,required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError("Never overwrite a simulation candidate")
    if bool(args.parent) == bool(args.input_bundle):
        parser.error("Choose exactly one historical --parent or portable --input-bundle")
    if args.input_bundle:
        bundle = read(args.input_bundle)
        rows, initial = bundle["rows"], bundle["initial_inventory"]
        values, timing = dict(bundle["costs"]), bundle["control_cost_rows"]
        source_hash = hashlib.sha256(args.input_bundle.read_bytes()).hexdigest()
        source_kind = bundle["kind"]
        seed = args.seed
    else:
        job = args.parent/"P-phase-release-parent-new-poisson300-01"
        before = [json.loads(line) for line in (job/"B-before/baseline/requests.jsonl").read_text(encoding="utf-8").splitlines()]
        by_id = {r["id"]:r for r in before}
        protocol = read(job/"protocol.json")
        rows = [dict(id=key,task=by_id[key]["task"],tokens=by_id[key]["P"]["prompt_tokens"],
                     hashes=by_id[key]["P"]["observation"]["full_prompt_hashes"][
                         :(by_id[key]["P"]["prompt_tokens"]-1)//128])
                for key in protocol["test_order"]]
        initial = read(job/"B-before/baseline/initial-inventory.json")
        values = dict(read(job/"cost-model.json")["costs"])
        timing = []
        for arm in ("B-before","T-parent","T-phase","B-after"):
            timing.extend(json.loads(line) for line in (job/arm/"baseline/requests.jsonl").read_text(encoding="utf-8").splitlines())
        source_hash = hashlib.sha256((job/"protocol.json").read_bytes()).hexdigest()
        source_kind = "historical-phase-release-parent"
        seed = args.parent/"calibration-seed"
    values["service"] = tuple(core.ObservedLatencyModel(**r) for r in values["service"])
    values["decode_seconds"] = tuple(values["decode_seconds"])
    costs = Costs(**values)
    output = {"kind":"causal-policy-feedback-coarse-screen-not-real-performance",
        "policy_does_not_observe_future_world_labels":True,"no_test_task_rate_thresholds":True,
        "no_assumed_RDMA_congestion":True,"native_source_order_and_publication_pending_real_validation":True,
        "world_service_labels":"isolated32-only; no test-path service oracle",
        "RPC_control_world":"aggregate of380 previously completed requests, never per-request lookup",
        "source_inputs_sha256":source_hash,"source_input_kind":source_kind,"cases":[]}
    args.output.parent.mkdir(parents=True,exist_ok=True)
    for residual in (False,True):
        for rate in RATES:
            case = {"rate":rate,"residual_world":residual,"arms":{}}
            for mode in ("native","ours"):
                case["arms"][mode] = Simulation(rows,initial,costs,seed,timing,rate,mode,residual).run()
            b,t = (case["arms"][m]["TTFT"]["p99"] for m in ("native","ours"))
            case["descriptive_p99_gain"] = 1-t/b
            output["cases"].append(case)
            print(json.dumps({"rate":rate,"residual":residual,"B_p99":b,"T_p99":t,"gain":1-t/b}),flush=True)
    with args.output.open("x",encoding="utf-8") as stream:
        json.dump(output,stream,indent=2)


if __name__ == "__main__":
    main()
