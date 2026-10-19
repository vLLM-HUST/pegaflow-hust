"""Immutable three-rate native-cache versus pending-tail shared-stack comparison."""
from __future__ import annotations
import argparse
import contextlib
import json
from pathlib import Path
import signal
import sys
import time
from common import ROOT,P_IP,D_IP,AGENT_PORT,META_GRPC,META_HTTP,dump,rpc,file_sha,assert_port_free
from confirmation_control import ARMS,SERVING_PHASE
from confirmation_state import allocation_identity,require_same,state_digest
from confirm_node import source_identity,PORTS
from arrivals import offsets
import run as core

RATES = {"150":.24516170769071072,"300":.49032341538142143,"600":.9806468307628429}

def read(path):
    return json.loads(path.read_text(encoding="utf-8"))

def copy_seed(destination):
    destination.mkdir(exist_ok=False)
    paths = sorted((ROOT/"calibration-seed").glob("calibration-*.json"))
    if len(paths)!=32:
        raise ValueError("Original32 isolated calibration required")
    for path in paths:
        with (destination/path.name).open("xb") as stream:
            stream.write(path.read_bytes())
    return {p.name:file_sha(p) for p in sorted(destination.glob("*.json"))}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output",type=Path,required=True)
    args = parser.parse_args()
    signal.signal(signal.SIGTERM,lambda *_:sys.exit(143))
    args.output.mkdir(parents=True,exist_ok=True)
    if (args.output/"protocol.json").exists():
        raise FileExistsError("Never reuse an experiment directory")
    acquired = False
    try:
        sources = source_identity()
        state = rpc(f"http://{D_IP}:{AGENT_PORT}/status",timeout=5)
        if not (state.get("ready") is True and state.get("cursor")==0 and state.get("phase") is None
                and state.get("children")==[] and state.get("root")==str(ROOT)):
            raise ValueError("D is not a fresh owned controller")
        require_same(sources,state["confirmation_sources"],"shared sources")
        acquired = True
        identity = {"P":{"allocation":allocation_identity(),"sources":sources},
                    "D":rpc(f"http://{D_IP}:{AGENT_PORT}/identity",timeout=30)}
        dump(args.output/"identity-initial.json",identity)
        manifest = read(ROOT/"workload/manifest.json")
        require_same(manifest["requests_sha256"],file_sha(ROOT/"workload/requests.jsonl"),"input SHA")
        rows = [json.loads(line) for line in (ROOT/"workload/requests.jsonl").read_text(encoding="utf-8").splitlines()]
        test = core.causal_order(rows,manifest["test_tasks"])
        if len(test)!=95 or len({r["task"] for r in test})!=8:
            raise ValueError("Keep the original95/8 replay")
        warmup = [next(r for r in rows if r["task"]==task) for task in manifest["calibration_tasks"]]
        template = read(ROOT/"reference-protocol.json")
        require_same(template["test_order"],[r["id"] for r in test],"frozen test order")
        values = dict(read(ROOT/"reference-cost-model.json")["costs"])
        values["service"] = tuple(core.ObservedLatencyModel(**r) for r in values["service"])
        values["decode_seconds"] = tuple(values["decode_seconds"])
        costs = core.Costs(**values)
        template.update(candidate_revision="native-vs-pending-shared-01",confirmation_arms=ARMS,
            serving_phase=SERVING_PHASE,source_manifest=manifest,shared_sources=sources,
            cost_sha256=file_sha(ROOT/"reference-cost-model.json"),frozen_before_test=False,
            exposed_development_replay=True,objective_metric="ttft_seconds",
            objective="nearest-rank conventional causal-ready-to-first p99 including all control and migration",
            baseline="round-robin plus default PegaFlow lookup; no selected cap or explicit migration",
            treatment="full detached pending-first calendar, source/boundary selection; slow OFF",
            same_host_authority_fixture=True,not_pure_vllm_system_comparison=True,
            native_implicit_RDMA_audit_required=True,independent_dual_node_audit_pending=True,
            rates=RATES,all_control_and_final_drain_included=True)
        dump(args.output/"protocol.json",template)
        common_state,common_warm,global_outputs = None,None,None
        before = {}
        def check(label):
            current = {"P":{"allocation":allocation_identity(),"sources":source_identity()},
                       "D":rpc(f"http://{D_IP}:{AGENT_PORT}/identity",timeout=30)}
            dump(args.output/("identity-"+label+".json"),current)
            require_same(identity,current,"same allocation and sources across cases")
        for arm,phase in ARMS:
            case = arm.rsplit("-",1)[1]
            check(arm+"-before")
            armroot = args.output/arm
            armroot.mkdir(exist_ok=False)
            seed = copy_seed(armroot/"operation-seed")
            dump(armroot/"operation-seed-manifest.json",seed)
            directory,backend,workers,meta,log = core.services(SERVING_PHASE,armroot)
            try:
                empty = [backend.inventory(n) for n in range(4)]
                dump(directory/"prewarmup-inventory.json",empty)
                state_digest(empty,empty=True)
                dump(directory/"counters-before.json",[backend.rdma_bytes(n) for n in range(4)])
                warmed = core.warmups(backend,warmup,directory)
                tokens = [r["cold"]["output_token_ids"] for r in warmed]
                inventory = [backend.inventory(n) for n in range(4)]
                dump(directory/"initial-inventory.json",inventory)
                digest = state_digest(inventory)
                if common_state is None:
                    common_state,common_warm = digest,tokens
                require_same(common_state,digest,"strict actual initial physical cache state")
                require_same(common_warm,tokens,"same warmup output")
                dump(directory/"initial-state-contract.json",dict(digest=digest,reference=common_state,
                    empty_before_warmup_passed=True,serving_phase=SERVING_PHASE,policy_phase=phase))
                times = offsets(95,RATES[case],1.,20261014)
                dump(directory/"formal-arm-started.json",dict(arm=arm,phase=phase,started_unix=time.time(),
                    four_ready=True,rate=RATES[case],offered_offsets=times,protocol_sha256=file_sha(args.output/"protocol.json")))
                result = core.formal(phase,backend,test,costs,1/RATES[case],directory,
                    baseline=before.get(case),pipeline=True,arrival_offsets=times,causal_arrivals=True,
                    event_queue_policy=True,slow_policy="off",operation_variant="phase",
                    naive_native=phase=="baseline")
                outputs = {r["id"]:r["D"]["output_token_ids"] for r in result}
                if global_outputs is None:
                    global_outputs = outputs
                require_same(global_outputs,outputs,"all cases exact95x128 outputs")
                if phase=="baseline":
                    before[case] = {r["id"]:r for r in result}
                dump(directory/"counters-after.json",[backend.rdma_bytes(n) for n in range(4)])
                dump(directory/"final-inventory.json",[backend.inventory(n) for n in range(4)])
            finally:
                backend.close()
                core.close_services(SERVING_PHASE,workers,meta,log)
            assert_port_free(P_IP,(*PORTS,META_GRPC,META_HTTP))
            check(arm+"-after")
        rpc(f"http://{D_IP}:{AGENT_PORT}/finish",{},timeout=120)
        dump(args.output/"pair-complete.json",dict(completed=True,arms=ARMS,independent_audit_pending=True))
    except BaseException as error:
        dump(args.output/"failure.json",dict(type=type(error).__name__,error=str(error),formal_valid=False))
        if acquired:
            with contextlib.suppress(OSError):
                rpc(f"http://{D_IP}:{AGENT_PORT}/abort",{},timeout=120)
        raise

if __name__=="__main__":
    main()
