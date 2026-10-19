"""Tail profile policy with physical context-dependent full D occupancy."""
from __future__ import annotations

from dataclasses import replace

from pegaflow.joint_policy import copy_package
from pegaflow.operation_schedule import schedule
from pegaflow.resource_profile import ProfileQueues, plan_profile, profile
from pegaflow.resource_queue import build_work


def context_work(key,order,offered,demand,route,layout,costs):
    work=build_work(key,order,offered,demand,route,layout,costs)
    tail=costs.decode_tail(route.d,demand.tokens)
    return replace(work,decode=tuple(replace(o,seconds=o.first_seconds+tail)
        if o.name=="D_api" else o for o in work.decode))


class ContextProfileQueues(ProfileQueues):
    work_builder=staticmethod(context_work)

    def reserve(self,key,route,now):
        with self.guard:
            super().reserve(key,route,now)
            demand,layout,costs,offered,_=self.context
            self.work[key]=self.work_builder(key,self.jobs[key].order,offered,demand,route,layout,costs)

    def revise(self,key,route,layout,costs):
        with self.guard:
            super().revise(key,route,layout,costs)
            w=self.work[key]
            if "D_api" not in w.completed_operations and "D_api" not in dict(w.active_operations):
                tail=costs.decode_tail(route.d,self.demands[key].tokens)
                self.work[key]=replace(w,decode=tuple(replace(o,seconds=o.first_seconds+tail)
                    if o.name=="D_api" else o for o in w.decode))

    def decode_boundary(self,key,route,layout,costs,now,max_candidates=64):
        with self.guard:
            item,current=self.demands[key],self.work[key]
            limit=max(layout.prefix(route.p,item.hashes),layout.prefix(route.d,item.hashes))
            lengths=tuple(dict.fromkeys((layout.prefix(route.d,item.hashes),limit,0,
                *range(limit-1,0,-1))))
            best=None
            for length in lengths[:max_candidates]:
                payload=0
                if length>layout.prefix(route.d,item.hashes):
                    try:
                        payload=copy_package(layout,route.p,route.d,item.hashes[:length]).operations[-1].payload_bytes
                    except ValueError:
                        continue
                candidate=replace(route,d_blocks=length)
                w=self.work_builder(key,current.order,current.offered,item,candidate,layout,costs)
                w=replace(w,p_done=current.p_done,d_enqueued=current.d_enqueued,
                    p_started=current.p_started,d_started=current.d_started,
                    actual_first=current.actual_first,completed_operations=current.completed_operations,
                    active_operations=current.active_operations)
                work=tuple(w if value.key==key else value for value in self.work.values())
                prediction=schedule(work,now,self._remaining_operation)
                seconds=costs.compute(route.d,item.tokens,length*item.block_tokens)+costs.copy_seconds(route.p,route.d,payload)
                order=(profile(work,prediction),payload,length)
                if best is None or order<best[0]:best=order,length,seconds
            if best is None:raise ValueError("No safe context-aware decode boundary")
            return best[1:]


def plan_context(layout,history,queues,costs,**options):
    options.setdefault("work_builder",context_work)
    return plan_profile(layout,history,queues,costs,**options)
