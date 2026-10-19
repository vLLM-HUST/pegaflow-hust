"""Completed-only local-level state for nonstationary physical stage costs.

The local-level model is y[k]=x[k]+e[k], x[k]=x[k-1]+u[k]. Noise variances
come from increment variance and lag-one covariance, not a chosen forgetting
factor. These are modelling assumptions, not identified hardware causes.
"""
from __future__ import annotations

from dataclasses import dataclass
import math
import statistics

from pegaflow.operation_context import ContextCosts, ContextOperationCosts
from pegaflow.operation_cost import prefill_features


class LocalLevel:
    def __init__(self):
        self.values = []
        self.level, self.variance, self.process, self.measurement = 1.0, 0.0, 0.0, 0.0

    def add(self, value):
        if not math.isfinite(value) or value < 0:
            raise ValueError("Completed finite nonnegative cost ratio required")
        self.values.append(value)
        if len(self.values) < 4:
            self.level = statistics.mean(self.values)
            self.variance = statistics.variance(self.values)/len(self.values) if len(self.values)>1 else 0
            return
        delta = [b-a for a, b in zip(self.values, self.values[1:])]
        # Cov(d[k],d[k-1])=-R and Var(d[k])=Q+2R under local-level assumptions.
        left, right = delta[:-1], delta[1:]
        covariance = sum((a-statistics.mean(left))*(b-statistics.mean(right))
                         for a,b in zip(left,right))/len(left)
        self.measurement = max(0.0, -covariance)
        self.process = max(0.0, statistics.pvariance(delta)-2*self.measurement)
        prior = self.variance+self.process
        gain = prior/(prior+self.measurement) if prior+self.measurement else 1/len(self.values)
        self.level = max(0.0, self.level+gain*(value-self.level))
        self.variance = (1-gain)*prior

    def snapshot(self):
        return dict(level=self.level, variance=self.variance, process_variance=self.process,
                    measurement_variance=self.measurement, observations=len(self.values))


@dataclass(frozen=True)
class StateCosts(ContextCosts):
    stage_states: tuple = ()

    def restore(self, node, prefix):
        return super().restore(node,prefix)*self.stage_states[node][0]["level"]

    def compute(self, node, tokens, prefix):
        old = self.original.service[node]
        body = self._dot(self.fits[node][1],prefill_features(tokens,prefix,self.block_tokens),
            old.intercept_seconds+old.seconds_per_uncached_token*(tokens-prefix))
        return (self.restore(node,prefix)+body*self.stage_states[node][1]["level"]+
                self.fits[node][2]+(self.fits[node][4] if node<2 else 0))

    def decode_tail(self,node,prompt_tokens):
        raw = super().decode_tail(node,prompt_tokens)-self.fits[node][4]
        return raw*self.stage_states[node][2]["level"]+self.fits[node][4]


class StateOperationCosts(ContextOperationCosts):
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.calibration = None
        self.seed = []
        self.states = [[LocalLevel() for _ in range(3)] for _ in range(4)]

    def _update_state(self,node,result,completed_seconds):
        raw=result["raw_metrics"]
        q,s,f,e=(raw[k] for k in ("queued_ts","scheduled_ts","first_token_ts","last_token_ts"))
        L,H=result["prompt_tokens"],result["num_cached_tokens"]
        base=self.calibration
        if H:
            predicted=base.restore(node,H)
            if predicted>0:self.states[node][0].add((s-q)/predicted)
        predicted=base.compute(node,L,H)-base.restore(node,H)-base.fits[node][2]
        if node<2:predicted-=base.fits[node][4]
        if predicted>0:self.states[node][1].add((f-s)/predicted)
        if node>=2:
            predicted=base.decode_tail(node,L)-base.fits[node][4]
            if predicted>0:self.states[node][2].add((e-f)/predicted)

    def observe(self,node,result,wire,completed_seconds,*,calibration=False):
        with self.guard:
            if self.calibration is None and not calibration:
                raise ValueError("Seal original calibration before online state observations")
            if self.calibration is not None and calibration:
                raise ValueError("Calibration is immutable after sealing")
            super().observe(node,result,wire,completed_seconds,calibration=calibration)
            if calibration:self.seed.append((node,result))
            else:self._update_state(node,result,completed_seconds)

    def seal_calibration(self):
        with self.guard:
            if self.calibration is not None:raise ValueError("Already sealed")
            self.calibration=super().freeze(0)
            for node,result in self.seed:self._update_state(node,result,0)

    def freeze(self,now):
        with self.guard:
            if self.calibration is None:return super().freeze(now)
            current=super().freeze(now)
            # Physical feature coefficients stay original-calibration-only.
            # Control means/copy observations and latent levels use own past.
            fits=tuple((self.calibration.fits[n][0],self.calibration.fits[n][1],
                *current.fits[n][2:]) for n in range(4))
            return StateCosts(**{**current.__dict__,"fits":fits,
                "decode_fits":self.calibration.decode_fits},
                stage_states=tuple(tuple(s.snapshot() for s in node) for node in self.states))

    def snapshot(self):
        with self.guard:
            return {**super().snapshot(),"kind":"completed-only-local-level-stage-state",
                "fixed_physical_seed":self.calibration is not None,
                "stage_states":[[s.snapshot() for s in node] for node in self.states],
                "noise_variances_data_derived":True,"forgetting_factor":None,
                "external_contention_cause_identified":False}
