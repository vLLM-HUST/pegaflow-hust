"""Independent native/controlled six-arm evidence audit; no hardware-clean claim."""
import argparse
from collections import Counter
import hashlib
import json
import math
from pathlib import Path
import re
from confirmation_control import ARMS
from confirmation_state import state_digest

def read(path):
    return json.loads(path.read_text(encoding="utf-8"))

def require(condition,label):
    if not condition:
        raise ValueError(label)

def quantiles(values):
    values = sorted(values)
    return {"mean":sum(values)/len(values),**{f"p{p}":values[math.ceil(p*len(values)/100)-1]
        for p in (50,90,95,99)}}

def audit(p,d,workload):
    for job in (p,d):
        result = read(job/"result.json")
        require(result["exit_code"]==0 and result["stop_reason"] is None,"supervisor result")
        require(read(job/"identity-initial.json")==read(job/"identity-final.json"),"whole-run identity")
    require(read(p/"pair-complete.json")["completed"] is True,"P completion")
    require(read(d/"node-complete.json")["finished"]==6,"D completion")
    protocol = read(p/"protocol.json")
    known = {r["id"]:r for r in (json.loads(line) for line in (workload/"requests.jsonl").read_text(encoding="utf-8").splitlines())}
    require(hashlib.sha256((workload/"requests.jsonl").read_bytes()).hexdigest()==
            protocol["source_manifest"]["requests_sha256"],"workload SHA")
    initial,outputs,configs,sources = None,None,None,None
    arm_results = {}
    for arm,phase in ARMS:
        native = phase=="baseline"
        directory = p/arm/"baseline"
        state_digest(read(directory/"prewarmup-inventory.json"),empty=True)
        actual_state = state_digest(read(directory/"initial-inventory.json"))
        if initial is None:
            initial = actual_state
        require(initial==actual_state,"strict initial physical cache state")
        rows = [json.loads(line) for line in (directory/"requests.jsonl").read_text(encoding="utf-8").splitlines()]
        require(len(rows)==95 and set(r["id"] for r in rows)==set(protocol["test_order"]),"95 request identities")
        implicit,explicit = [0]*4,[0]*4
        offered = read(directory/"formal-arm-started.json")["offered_offsets"]
        for row in rows:
            require(row["policy_native_cache"] is native,"controller cache mode")
            require(row["offered_arrival_seconds"]==offered[protocol["test_order"].index(row["id"])],"same nominal stream")
            prompt = hashlib.sha256(json.dumps(known[row["id"]]["prompt_token_ids"],separators=(",",":")).encode()).hexdigest()
            for role in ("P","D"):
                result = row[role]
                require(result["prompt_token_ids_sha256"]==prompt,"actual input token digest")
                require(result["policy_native_cache"] is native,"worker cache mode")
                require(result["output_sha256"]==hashlib.sha256(json.dumps(result["output_token_ids"]).encode()).hexdigest(),"output hash")
                require(len(result["output_token_ids"])==(1 if role=="P" else 128),"full output budget")
                require(result["explicit_restore_limit"] is None if native else type(result["explicit_restore_limit"]) is int,"restore cap contract")
                require(native or result["rdma_bytes"]==0,"unpriced controlled implicit RDMA")
                node = result["slot"]+(0 if role=="P" else 2)
                implicit[node] += result["rdma_bytes"]
                timing = row["pipeline_timing"]
                require(timing[role+"_enqueued_seconds"]<=timing[role+"_started_seconds"]<=timing[role+"_finished_seconds"],"FIFO timestamps")
            require(row["P"]["output_token_ids"][0]==row["D"]["output_token_ids"][0],"P/D first token")
            require(row["arrival_seconds"]>=row["offered_arrival_seconds"],"causal admission")
        produced = {r["id"]:r["D"]["output_token_ids"] for r in rows}
        if outputs is None:
            outputs = produced
        require(outputs==produced,"six-arm exact output equality")
        for node in range(4):
            same = sorted((r["pipeline_timing"]["P_started_seconds" if node<2 else "D_started_seconds"],
                           r["pipeline_timing"]["P_finished_seconds" if node<2 else "D_finished_seconds"])
                          for r in rows if r["route"]["p" if node<2 else "d"]==node)
            require(all(a[1]<=b[0] for a,b in zip(same,same[1:])),"nonoverlapping executor")
        for event in read(directory/"cache-actions.json"):
            if event["kind"]!="copy":
                continue
            receipt,request = event["receipt"],event["request"]
            require(receipt["epoch"]==request["epoch"] and receipt["generation_before"]==request["generation"],"copy generation/epoch")
            require(receipt["generation_after"]>=receipt["generation_before"] and receipt["allocated_before_rdma"] is True
                    and receipt["published_locally"] is True,"allocation-before-copy publication receipt")
            require(receipt["staging_peak_bytes"]>=receipt["allocated_bytes"]>=receipt["payload_bytes"],"rounded physical/staging accounting")
            explicit[event["operation"]["destination"]] += receipt["payload_bytes"]
        before,after = (read(directory/name) for name in ("counters-before.json","counters-after.json"))
        require([b-a for a,b in zip(before,after)]==[a+b for a,b in zip(implicit,explicit)],"all real RDMA receiver counter reconciliation")
        node_sources = []
        for node in range(4):
            role,slot = ("P",node) if node<2 else ("D",node-2)
            worker = (p if node<2 else d)/arm/"workers/baseline"/(role+str(slot))
            persisted = [read(path) for path in worker.glob("strategy-request-*.json")]
            for row in rows:
                if row[role]["slot"]!=slot:
                    continue
                matching = [value for value in persisted if value["id"]==row["id"]]
                require(len(matching)==1 and matching[0]==row[role],"independent actual worker request equality")
            config = read(worker/"configuration.json")
            transfer = config.pop("kv_transfer_config")
            require(config["tensor_parallel_size"]==1 and config["block_size"]==128 and
                    config["kv_cache_memory_bytes"]==2*1024**3,"actual serving budget")
            if configs is None:
                configs = config
            require(configs==config,"same nonconnection serving configuration")
            authority = read(worker/"prefix-authority.json")
            require(authority["host_inventory_authority"] is True and authority["native_lookup_without_policy_cap"] is True,"common authority/native contract")
            provenance = read(worker/"provenance.json")["sources"]
            normalized = {k:(v["sha256"] if isinstance(v,dict) else v) for k,v in provenance.items()}
            if sources is None:
                sources = normalized
            require(sources==normalized,"same actual serving source/native binary")
            text = (worker/"native.log").read_text(encoding="utf-8",errors="strict")
            peers = re.findall(r"RDMA fetch summary:.*?remote=([^ ]+)",text)
            require(all(peer in ("10.17.119.1:50300","10.17.119.1:50301","10.17.119.71:50300","10.17.119.71:50301") for peer in peers),"owned RDMA sources only")
            require(after[node]==0 or len(peers)>0,"nonzero receiver requires real peer provenance")
            node_sources.append(dict(node=node,peer_counts=dict(Counter(peers))))
        arm_results[arm] = {"TTFT":quantiles([r["ttft_seconds"] for r in rows]),
            "nominal":quantiles([r["offered_to_first_seconds"] for r in rows]),
            "phase":read(directory/"phase-complete.json"),"RDMA_sources":node_sources}
    return dict(contract_audit_passed=True,clean_hardware_certified=False,
                statistical_scope="95 exposed requests, one seed, nearest-rank p99=max; descriptive",
                arms=arm_results)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--p",type=Path,required=True)
    parser.add_argument("--d",type=Path,required=True)
    parser.add_argument("--workload",type=Path,required=True)
    parser.add_argument("--output",type=Path,required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError("Preserve old audits")
    result = audit(args.p,args.d,args.workload)
    with args.output.open("x",encoding="utf-8") as stream:
        json.dump(result,stream,indent=2)
    print(json.dumps(result,indent=2))

if __name__=="__main__":
    main()
