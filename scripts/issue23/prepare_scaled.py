"""Freeze distinct benchmark cases with complete fixed-size checkpoint inputs."""

import argparse
import json
import os
import random
from collections import Counter
from pathlib import Path

from common import read_json, sha256, write_json
from prepare_data import mmlu_messages

os.environ["USE_TORCH"] = "0"
os.environ["USE_TF"] = "0"


def prepare(raw, model, output, count=1024, cohort_size=8):
    from transformers import AutoTokenizer

    if count % cohort_size:
        raise ValueError("Request count must be a multiple of cohort_size")
    tokenizer = AutoTokenizer.from_pretrained(model, local_files_only=True)
    exported = output / "mmlu-records.json"
    if exported.exists():
        records = read_json(exported)
        validation, mmlu = records["validation"], records["test"]
    else:
        import pyarrow.parquet as pq

        validation = pq.read_table(
            raw / "mmlu-pro/data/validation-00000-of-00001.parquet"
        ).to_pylist()
        mmlu = pq.read_table(
            raw / "mmlu-pro/data/test-00000-of-00001.parquet"
        ).to_pylist()
    examples = {}
    for row in validation:
        examples.setdefault(row["category"], []).append(row)
    frontier = [
        json.loads(line)
        for line in (raw / "frontierscience/olympiad/test.jsonl")
        .read_text()
        .splitlines()
        if line.strip()
    ]
    random.Random(17).shuffle(mmlu)
    candidates = [("frontierscience", row) for row in frontier]
    categories = {}
    for row in mmlu:
        categories.setdefault(row["category"], []).append(row)
    for index in range(max(len(rows) for rows in categories.values())):
        for category in sorted(categories):
            if index < len(categories[category]):
                candidates.append(("mmlu-pro", categories[category][index]))
    cases = []
    filler = tokenizer.encode(" reference", add_special_tokens=False)[0]
    for dataset, row in candidates:
        messages = (
            mmlu_messages(row, examples[row["category"]][:5])
            if dataset == "mmlu-pro"
            else [{"role": "user", "content": row["problem"]}]
        )
        messages[-1]["content"] += (
            "\n\n<benchmark_context>\nAuxiliary context; use the question above to answer.\n"
            "</benchmark_context>\nAnswer the original question."
        )
        rendered = tokenizer.apply_chat_template(
            messages, tokenize=False, add_generation_prompt=True, enable_thinking=False
        )
        marker = rendered.rindex("</benchmark_context>")
        prefix = tokenizer.encode(rendered[:marker], add_special_tokens=False)
        suffix = tokenizer.encode(rendered[marker:], add_special_tokens=False)
        padding = 4097 - len(prefix) - len(suffix)
        if padding < 0:
            continue
        index = len(cases)
        rid = f"issue23-scaled-{index:05d}"
        ids = prefix + [filler] * padding + suffix
        cases.append(
            {
                "request_id": rid,
                "cohort_id": f"scaled-{index // cohort_size:04d}",
                "smooth_slot": index % cohort_size,
                "planned_offset_seconds": 0,
                "dataset": dataset,
                "category": row.get("category", row.get("subject")),
                "source_id": str(row.get("question_id", row.get("task_group_id"))),
                "input_tokens": len(ids),
                "context_extension": "reference tokens inside benchmark_context",
                "payload": {
                    "model": "hybrid-qwen35",
                    "prompt": ids,
                    "request_id": rid,
                    "stream": True,
                    "stream_options": {"include_usage": True},
                    "temperature": 0.0,
                    "seed": 17,
                    "max_tokens": 1,
                    "ignore_eos": True,
                    "return_token_ids": True,
                    "cache_salt": rid,
                    "add_special_tokens": False,
                },
            }
        )
        if len(cases) == count:
            break
    if len(cases) != count:
        raise ValueError("Insufficient eligible benchmark cases")
    random.Random(23).shuffle(cases)
    for index, case in enumerate(cases):
        case.update(
            cohort_id=f"scaled-{index // cohort_size:04d}",
            smooth_slot=index % cohort_size,
        )
    write_json(output / "cases.json", cases)
    write_json(
        output / "contract.json",
        {
            "protocol_id": "issue23-scaled-eight-v1",
            "cohort_size": cohort_size,
            "smooth_interval_ms": 1200,
            "rdma_rate_bps": 1_000_000_000,
            "requests_per_arm": count,
            "input_tokens": 4097,
            "reused_tokens": 4096,
            "output_tokens": 1,
            "background_decode_requests": 4,
            "datasets": dict(Counter(c["dataset"] for c in cases)),
            "cases_sha256": sha256(output / "cases.json"),
            "source_model": str(model),
        },
    )
    return read_json(output / "contract.json")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw", type=Path, required=True)
    parser.add_argument("--model", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--count", type=int, default=1024)
    parser.add_argument("--cohort-size", type=int, default=8)
    args = parser.parse_args()
    print(prepare(args.raw, args.model, args.output, args.count, args.cohort_size))
