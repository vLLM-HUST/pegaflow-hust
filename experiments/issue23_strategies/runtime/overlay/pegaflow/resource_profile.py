"""Lexicographic tail and release profiles for critical-path slack decisions.

No load-balancing quota or task/rate coefficient is introduced. The leading
component remains the existing admitted-work p99. Later ranks and releases
only decide a true tie in that component. Actual first-token completions stay
immutable, and their decode tails keep holding the original resources.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
import itertools
import time

from pegaflow.joint_policy import Copy, Plan, Transition, neighbors
from pegaflow.operation_schedule import merge_operations, schedule
from pegaflow.resource_placement import relevant_history
from pegaflow.resource_queue import ResourceQueues, ResourceRoute, build_work, direction, objective


def profile(work, prediction):
    def ordered(field):
        return tuple(sorted((prediction[field][w.key]-w.offered for w in work), reverse=True))
    return objective(work, prediction), ordered("first"), ordered("full"), ordered("P_end")


@dataclass(frozen=True)
class ProfileRoute(ResourceRoute):
    predicted_tail_profile: tuple = ()
    predicted_full_profile: tuple = ()
    predicted_P_release_profile: tuple = ()


def route_order(route):
    return (route.predicted_system_p99, route.predicted_tail_profile,
            route.predicted_full_profile, route.predicted_P_release_profile,
            route.extra_bytes, route.p, route.d, route.source, route.p_blocks, route.d_blocks)


class ProfileQueues(ResourceQueues):
    work_builder = staticmethod(build_work)

    def forecast(self, route, now):
        with self.guard:
            demand, layout, costs, offered, origin = self.context
            candidate = self.work_builder("__candidate__", self.sequence, offered, demand, route, layout, costs)
            work = (*self.work.values(), candidate)
            predicted = schedule(work, origin, self._remaining_operation)
            value = profile(work, predicted)
            return ProfileRoute(**{**route.__dict__,
                "predicted_p_end":predicted["P_end"][candidate.key],
                "predicted_first":predicted["first"][candidate.key],
                "predicted_end":predicted["full"][candidate.key]},
                predicted_system_p99=value[0], predicted_tail_profile=value[1],
                predicted_full_profile=value[2], predicted_P_release_profile=value[3],
                operation_model_version=costs.version,
                model_latest_completed_seconds=costs.latest_completed_seconds,
                forecast_origin_seconds=origin,
                objective_scope="admitted-live-p99-then-tail-and-resource-release-profiles")


def transition_profile(transition, work, demands, routes, costs, now, remaining, planning=0.0,
                       work_builder=build_work):
    nodes, seconds, resources = set(), 0.0, set()
    for op in transition.operations:
        if isinstance(op, Copy):
            nodes.update((op.source, op.destination))
            resources.add("link:"+direction(op.source,op.destination))
            seconds += costs.copy_seconds(op.source,op.destination,op.payload_bytes)
        else:
            nodes.add(op.node); seconds += costs.eviction_fixed
    resources.update(f"gate{n}" for n in nodes)
    if any(set(o.resources)&resources for w in work for o in (*w.prefill,*w.decode)
           if o.name in dict(w.active_operations)):
        return None
    revised=[]
    for w in work:
        new=work_builder(w.key,w.order,w.offered,demands[w.key],routes[w.key],transition.layout,costs)
        preserve=w.completed_operations|set(dict(w.active_operations))
        def chain(operations,previous):
            return merge_operations(previous,operations,preserve)
        revised.append(replace(new,prefill=chain(new.prefill,w.prefill),decode=chain(new.decode,w.decode),
            p_done=w.p_done,d_enqueued=w.d_enqueued,p_started=w.p_started,d_started=w.d_started,
            actual_first=w.actual_first,completed_operations=w.completed_operations,
            active_operations=w.active_operations))
    # Planning is a conservative dispatch delay for unacquired operations;
    # acquired operations progress during it. Never shift observed firsts.
    prediction=schedule(revised,now+planning,remaining,
        {resource:now+planning+seconds for resource in resources})
    return profile(revised,prediction)


def plan_profile(layout, history, queues, costs, *, max_trials=256, clock=time.monotonic,
                 work_builder=build_work, planning_overhead=0.0, remaining_estimator=None):
    started=clock()
    with queues.guard:
        work=tuple(queues.work.values());demands=dict(queues.demands);routes=dict(queues.routes)
        version=queues.progress_version
        samples={k:tuple(v) for k,v in queues.operation_samples.items()}
        now=started-queues.started
    if not work:
        return Plan(Transition(layout),None,None,0,0,clock()-started,
            tuple(d.key for d in history),"no_admitted_work"),version,()
    def remaining(w,o,elapsed):
        if remaining_estimator is not None:
            return remaining_estimator(w,o,elapsed)
        node=w.p if o.name in ("history","P_api","P_admission") else w.d
        values=[a/p*o.seconds-elapsed for p,a in samples.get((node,o.name),())
                if p>0 and a/p*o.seconds>elapsed]
        return sum(values)/len(values) if values else max(0,o.seconds-elapsed)
    initial=transition_profile(Transition(layout),work,demands,routes,costs,now,remaining,
        work_builder=work_builder)
    best,value,evaluated=Transition(layout),initial,1
    trials=1
    for candidate in itertools.islice(neighbors(layout,relevant_history(history,demands.values())),max_trials-1):
        trials+=1
        if candidate is None or not candidate.layout.feasible():continue
        predicted=transition_profile(candidate,work,demands,routes,costs,now,remaining,clock()-started+planning_overhead,
            work_builder=work_builder)
        evaluated+=1
        if predicted is not None and predicted<value:best,value=candidate,predicted
    elapsed=clock()-started
    reason="candidate_budget_exhausted" if trials==max_trials else "neighborhood_exhausted"
    if best.operations:
        value=transition_profile(best,work,demands,routes,costs,now,remaining,elapsed+planning_overhead,
            work_builder=work_builder)
        if value is None or value>=initial:
            best,value,reason=Transition(layout),initial,"planning_cost_erases_profile_benefit"
    return Plan(best,initial,value,trials,evaluated,elapsed,tuple(d.key for d in history),reason),version,tuple(w.key for w in work)
