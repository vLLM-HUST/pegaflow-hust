"""Freeze dataset selection, rendered prompts and open-loop request schedules."""

import argparse
import hashlib
import importlib.metadata
import json
import os
import random
import sys
import urllib.request
from collections import Counter
from pathlib import Path

from common import distribution, read_json, sha256, write_json

os.environ["USE_TORCH"] = "0"
os.environ["USE_TF"] = "0"

MODEL_REVISION = "59d61f3ce65a6d9863b86d2e96597125219dc754"
DATASETS = {
    "mmlu-pro": ("MMLUPro", "TIGER-Lab/MMLU-Pro", "b189ec765aa7ed75c8acfea42df31fdae71f97be",
                 ["data/test-00000-of-00001.parquet", "data/validation-00000-of-00001.parquet", "README.md"]),
    "frontierscience": ("FrontierScience", "openai/frontierscience", "25ed67db7da8f4591484e764008ff585544f5a30",
                        ["olympiad/test.jsonl", "README.md"]),
}
CATEGORIES = ["chemistry", "computer science", "math", "physics"]


def verify_source(path, record):
    size = path.stat().st_size
    if size != record["size"]:
        return False
    if record.get("lfs"):
        return sha256(path) == record["lfs"]["oid"]
    return hashlib.sha1(f"blob {size}\0".encode() + path.read_bytes()).hexdigest() == record["oid"]


def sources(root, metadata):
    checked = []
    for name, (key, repo, revision, files) in DATASETS.items():
        records = {r["path"]: r for r in metadata[key]}
        for filename in files:
            path = root / "raw" / name / filename
            path.parent.mkdir(parents=True, exist_ok=True)
            url = f"https://huggingface.co/datasets/{repo}/resolve/{revision}/{filename}"
            if not path.exists():
                temporary = path.with_suffix(path.suffix + ".part")
                with urllib.request.urlopen(url, timeout=60) as response:
                    temporary.write_bytes(response.read())
                if not verify_source(temporary, records[filename]):
                    raise ValueError(f"Downloaded dataset content mismatch: {url}")
                temporary.replace(path)
            if not verify_source(path, records[filename]):
                raise ValueError(f"Dataset content mismatch: {path}")
            checked.append({"dataset": name, "repo": repo, "revision": revision,
                            "file": filename, "url": url, "bytes": path.stat().st_size,
                            "sha256": sha256(path), "official_content_matches": True})
    write_json(root / "checks/data-files.json", checked)


def question(row):
    options = "\n".join(f"{chr(65 + i)}. {value}" for i, value in enumerate(row["options"]))
    return f"Question: {row['question']}\nOptions:\n{options}\nAnswer:"


def mmlu_messages(row, examples):
    intro = (f"The following are multiple choice questions about {row['category']}. "
             "Explain your reasoning and finish with exactly 'The answer is (X)', "
             "where X is the option letter.")
    parts = [intro]
    for example in examples:
        reasoning = example["cot_content"]
        if reasoning.startswith("A: "):
            reasoning = reasoning[3:]
        parts.append(question(example) + " " + reasoning)
    parts.append(question(row))
    return [{"role": "user", "content": "\n\n".join(parts)}]


def render(tokenizer, messages, cohort, min_input_tokens=0):
    system = {"role": "system", "content": f"Issue23 workload cohort {cohort}.\nSolve the user's task."}
    messages = [system, *messages]
    prompt = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True,
                                           enable_thinking=False)
    ids = tokenizer.encode(prompt, add_special_tokens=False)
    if len(ids) < min_input_tokens:
        sentence = "This is an auxiliary reference passage used to keep the workload context length consistent across measurements. "
        prefix = "\n\n<benchmark_context>\nThis passage is unrelated to the original question. Ignore its contents when selecting the answer.\n"
        suffix = "\n</benchmark_context>\nNow solve the original question above, using its requested answer format."
        count = max(1, (min_input_tokens - len(ids)) // len(tokenizer.encode(sentence, add_special_tokens=False)))
        original = messages[-1]["content"]
        while len(ids) < min_input_tokens:
            messages[-1] = {**messages[-1], "content": original + prefix + sentence * count + suffix}
            prompt = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True,
                                                   enable_thinking=False)
            ids = tokenizer.encode(prompt, add_special_tokens=False)
            count += 1
    return messages, prompt, ids


def build_split(tokenizer, rows, split, dataset, examples):
    grouped = {}
    for row in rows:
        category = row["category"] if dataset == "mmlu-pro" else row["subject"]
        messages = mmlu_messages(row, examples[category]) if dataset == "mmlu-pro" else [
            {"role": "user", "content": row["problem"]}]
        length = len(render(tokenizer, messages, "length-probe")[2])
        grouped.setdefault(category, []).append((length, row, messages))
    cohorts = []
    for category in sorted(grouped):
        ordered = sorted(grouped[category], key=lambda item: (item[0], str(item[1].get("question_id", item[1].get("task_group_id")))))
        if len(ordered) % 4:
            raise ValueError(f"{split}/{category} is not a multiple of 4")
        cohorts.extend((category, ordered[i:i + 4]) for i in range(0, len(ordered), 4))
    random.Random(17).shuffle(cohorts)
    cases = []
    for cohort_index, (category, members) in enumerate(cohorts):
        cohort = f"{dataset}-{split}-{cohort_index:04d}"
        for slot, (_, row, messages) in enumerate(members):
            request_id = f"issue23-{dataset}-{split}-{cohort_index:04d}-{slot}"
            rendered_messages, prompt, ids = render(tokenizer, messages, cohort, min_input_tokens=4097)
            if len(ids) > 16384:
                raise ValueError(f"Selected prompt exceeds the frozen cap: {request_id}")
            max_tokens = 512 if dataset == "mmlu-pro" else 1024
            payload = {"model": "tailguard-qwen35", "prompt": prompt, "request_id": request_id,
                       "stream": True, "stream_options": {"include_usage": True}, "n": 1,
                       "temperature": 0.0, "top_p": 1.0, "top_k": -1, "presence_penalty": 0.0,
                       "seed": 17, "max_tokens": max_tokens, "add_special_tokens": False}
            cases.append({"request_id": request_id, "cohort_id": cohort, "smooth_slot": slot,
                          "planned_offset_seconds": cohort_index * 8, "dataset": dataset,
                          "split": split, "category": category,
                          "source_id": str(row.get("question_id", row.get("task_group_id"))),
                          "validation_example_ids": [r["question_id"] for r in examples[category]] if dataset == "mmlu-pro" else [],
                          "source_record": row, "reference_answer": row["answer"],
                          "messages": rendered_messages, "prompt": prompt,
                          "prompt_sha256": hashlib.sha256(prompt.encode()).hexdigest(),
                          "input_token_ids": ids, "input_tokens": len(ids), "payload": payload})
    return cases


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--model", required=True, type=Path)
    parser.add_argument("--pyarrow-path", type=Path)
    args = parser.parse_args()
    if args.pyarrow_path:
        sys.path.insert(0, str(args.pyarrow_path))
    import pyarrow.parquet as pq
    from transformers import AutoTokenizer

    root = args.root
    model_check = read_json(root / "checks/model-sha256.json")
    if model_check["status"] != "passed" or model_check["revision"] != MODEL_REVISION:
        raise ValueError("A completed official model verification is required")
    sources(root, read_json(root / "config/data-source-files.json"))
    tokenizer = AutoTokenizer.from_pretrained(str(args.model), local_files_only=True, trust_remote_code=False)
    test = pq.read_table(root / "raw/mmlu-pro/data/test-00000-of-00001.parquet").to_pylist()
    validation = pq.read_table(root / "raw/mmlu-pro/data/validation-00000-of-00001.parquet").to_pylist()
    examples = {category: sorted((r for r in validation if r["category"] == category),
                                key=lambda r: r["question_id"])[:5] for category in CATEGORIES}
    if any(len(value) != 5 for value in examples.values()):
        raise ValueError("Five validation examples are required for every category")
    rng = random.Random(17)
    chosen = {"main": [], "calibration": [], "smoke": []}
    for category in CATEGORIES:
        pool = sorted((r for r in test if r["category"] == category), key=lambda r: r["question_id"])
        rng.shuffle(pool)
        chosen["calibration"].extend(pool[:4])
        smoke_count = 4 if category in {"math", "physics"} else 0
        chosen["smoke"].extend(pool[4:4 + smoke_count])
        chosen["main"].extend(pool[4 + smoke_count:36 + smoke_count])
    frontier = [json.loads(line) for line in (root / "raw/frontierscience/olympiad/test.jsonl").read_text().splitlines() if line.strip()]
    frontier_selected = []
    frontier_rng = random.Random(17)
    for subject, count in [("physics", 8), ("chemistry", 4), ("biology", 4)]:
        pool = sorted((r for r in frontier if r["subject"] == subject), key=lambda r: r["task_group_id"])
        if len(pool) < count:
            raise ValueError(f"Insufficient FrontierScience {subject} questions")
        frontier_selected.extend(frontier_rng.sample(pool, count))
    outputs = {}
    for split, rows in chosen.items():
        outputs[split] = build_split(tokenizer, rows, split, "mmlu-pro", examples)
    outputs["frontier-smoke"] = build_split(tokenizer, frontier_selected, "smoke", "frontierscience", {})
    summary = {}
    for split, cases in outputs.items():
        path = root / "prepared" / f"{split}.json"
        write_json(path, cases)
        summary[split] = {"requests": len(cases), "cohorts": len(cases) // 4,
                          "categories": dict(Counter(c["category"] for c in cases)),
                          "input_tokens": distribution([c["input_tokens"] for c in cases]),
                          "sha256": sha256(path)}
    identifiers = [c["source_id"] for split in chosen for c in outputs[split]]
    if len(identifiers) != 152 or len(set(identifiers)) != 152:
        raise ValueError("MMLU split leakage or unexpected sample count")
    arms = ["rdma-smooth", "rdma-burst", "local_copy-smooth", "local_copy-burst"]
    ordering_rng = random.Random(17)
    ordering = []
    for _ in range(2):
        current = arms.copy()
        ordering_rng.shuffle(current)
        ordering.append(current)
    contract = {"schema_version": 1, "protocol_id": "issue23-qwen35-20261012",
                "model_path": str(args.model), "model_revision": MODEL_REVISION,
                "model_check_sha256": sha256(root / "checks/model-sha256.json"),
                "tokenizer_class": type(tokenizer).__name__,
                "libraries": {n: importlib.metadata.version(n) for n in ["transformers", "tokenizers", "jinja2", "pyarrow"]},
                "tokenizer_sha256": sha256(args.model / "tokenizer.json"),
                "template_sha256": sha256(args.model / "chat_template.jinja"),
                "sources_sha256": sha256(root / "checks/data-files.json"),
                "sampling_seed": 17, "cohort_size": 4, "cohort_period_seconds": 8,
                "timeout_seconds": 300, "qps_per_peer": 2, "enable_thinking": False,
                "min_input_tokens": 4097, "runtime_attention_block_tokens": 2048,
                "python_hash_seed": 17, "prefix_caching_hash_algo": "sha256",
                "context_protocol": "original benchmark question and examples followed by a fixed ignorable reference appendix",
                "tensor_parallel_size": 2, "max_model_len": 32768,
                "max_input_tokens": 16384, "max_num_seqs": 16, "max_num_batched_tokens": 4096,
                "device_cache_bytes_per_card": 16 * 1024**3, "host_pool_bytes_per_node": 30 * 1024**3,
                "tailguard_admission": False, "speculative_decoding": False,
                "prompt_protocol": "rendered completions; 5-shot MMLU validation examples; controlled cohort prefix",
                "validation_example_ids": {k: [r["question_id"] for r in v] for k, v in examples.items()},
                "split_summary": summary, "round_arm_order": ordering,
                "hardware_qualification": "pending"}
    write_json(root / "prepared/contract.json", contract)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
