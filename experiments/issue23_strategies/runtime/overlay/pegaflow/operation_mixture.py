"""Convex stage-cost aggregation learned from past pre-operation forecasts.

One coefficient per device/stage minimizes completed prequential square loss.
No task/rate switch, hand-picked forgetting factor or post-hoc node exception.
"""
from __future__ import annotations

from dataclasses import dataclass

from pegaflow.operation_context import ContextOperationCosts
from pegaflow.operation_state import StateCosts,StateOperationCosts


class ConvexFit:
    def __init__(self):
        self.numerator,self.denominator,self.observations=0.0,0.0,0

    def add(self,mean,state,actual):
        delta=state-mean
        self.numerator+=delta*(actual-mean)
        self.denominator+=delta*delta
        self.observations+=1

    @property
    def weight(self):
        # Equal experts is the symmetric uninformative initial condition.
        return min(1,max(0,self.numerator/self.denominator)) if self.denominator else .5


@dataclass(frozen=True)
class MixtureCosts(StateCosts):
    mean_costs: object = None
    state_weights: tuple = ()

    def restore(self,node,prefix):
        w=self.state_weights[node][0]
        return (1-w)*self.mean_costs.restore(node,prefix)+w*StateCosts.restore(self,node,prefix)

    def compute(self,node,tokens,prefix):
        control=self.fits[node][2]+(self.fits[node][4] if node<2 else 0)
        mean=self.mean_costs.compute(node,tokens,prefix)-self.mean_costs.restore(node,prefix)-control
        state=StateCosts.compute(self,node,tokens,prefix)-self.restore(node,prefix)-control
        w=self.state_weights[node][1]
        return self.restore(node,prefix)+(1-w)*mean+w*state+control

    def decode_tail(self,node,prompt_tokens):
        w=self.state_weights[node][2]
        return (1-w)*self.mean_costs.decode_tail(node,prompt_tokens)+w*StateCosts.decode_tail(self,node,prompt_tokens)


def stages(frozen,node,L,H,*,state=False):
    restore=StateCosts.restore(frozen,node,H) if state else frozen.restore(node,H)
    control=frozen.fits[node][2]+(frozen.fits[node][4] if node<2 else 0)
    # StateCosts.compute dispatches self.restore; keep that exact subtraction.
    first=StateCosts.compute(frozen,node,L,H) if state else frozen.compute(node,L,H)
    body=first-frozen.restore(node,H)-control
    tail=(StateCosts.decode_tail(frozen,node,L)-frozen.fits[node][4] if state else
          frozen.decode_tail(node,L)-frozen.fits[node][4]) if node>=2 else 0
    return restore,body,tail


class MixtureOperationCosts(StateOperationCosts):
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.mixes=[[ConvexFit() for _ in range(3)] for _ in range(4)]
        self.pending={}
        self.skipped_prefix_mismatch=0

    def begin_api(self,node,key,L,H,now):
        with self.guard:
            if (node,key) in self.pending:raise ValueError("Duplicate pre-operation forecast")
            frozen=self.freeze(now)
            self.pending[node,key]=(H,stages(frozen.mean_costs,node,L,H),stages(frozen,node,L,H,state=True))

    def observe(self,node,result,wire,completed_seconds,*,calibration=False):
        with self.guard:
            if not calibration:
                H,mean,state=self.pending.pop((node,result["id"]))
                raw=result["raw_metrics"]
                q,s,f,e=(raw[k] for k in ("queued_ts","scheduled_ts","first_token_ts","last_token_ts"))
                if H==result["num_cached_tokens"]:
                    for fit,a,b,y in zip(self.mixes[node],mean,state,(s-q,f-s,e-f)):
                        fit.add(a,b,y)
                else:self.skipped_prefix_mismatch+=1
            super().observe(node,result,wire,completed_seconds,calibration=calibration)

    def freeze(self,now):
        with self.guard:
            state=super().freeze(now)
            if self.calibration is None:return state
            mean=ContextOperationCosts.freeze(self,now)
            return MixtureCosts(**state.__dict__,mean_costs=mean,
                state_weights=tuple(tuple(f.weight for f in node) for node in self.mixes))

    def snapshot(self):
        with self.guard:
            return {**super().snapshot(),"kind":"prequential-convex-mean-state-aggregation",
                "weights":[[f.weight for f in node] for node in self.mixes],
                "prefix_mismatch_not_trained":self.skipped_prefix_mismatch,
                "pending_not_used_as_training":len(self.pending)}
