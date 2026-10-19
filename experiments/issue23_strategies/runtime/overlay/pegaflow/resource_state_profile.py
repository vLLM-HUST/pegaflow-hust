"""Refresh unstarted operation costs without rewriting actual progress."""
from __future__ import annotations

from dataclasses import replace
from pegaflow.operation_schedule import merge_operations

from pegaflow.resource_context_profile import ContextProfileQueues


def refresh_work(old,new):
    preserve=old.completed_operations|set(dict(old.active_operations))
    def chain(previous,updated):
        return merge_operations(previous,updated,preserve)
    return replace(new,prefill=chain(old.prefill,new.prefill),decode=chain(old.decode,new.decode),
        p_done=old.p_done,d_enqueued=old.d_enqueued,p_started=old.p_started,
        d_started=old.d_started,actual_first=old.actual_first,
        p_not_before=old.p_not_before,d_not_before=old.d_not_before,
        completed_operations=old.completed_operations,active_operations=old.active_operations)


class StateProfileQueues(ContextProfileQueues):
    def refresh(self,layout,costs):
        with self.guard:
            for key,old in tuple(self.work.items()):
                new=self.work_builder(key,old.order,old.offered,self.demands[key],self.routes[key],layout,costs)
                self.work[key]=refresh_work(old,new)

    def forecast(self,route,now):
        with self.guard:
            _,layout,costs,_,_=self.context
            self.refresh(layout,costs)
            return super().forecast(route,now)

    def decode_boundary(self,key,route,layout,costs,now,max_candidates=64):
        with self.guard:
            self.refresh(layout,costs)
            return super().decode_boundary(key,route,layout,costs,now,max_candidates)
