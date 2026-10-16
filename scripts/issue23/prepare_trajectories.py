"""Build fixed, full-visible-history ATIF snapshots for Qwen rate replay."""

import argparse
import hashlib
import json
from pathlib import Path

REVISION = "3f5b93ff1c5ea84b2c94cf479d0c942227191b4a"
SCOPE = [
    ("main", "distribution-search", "distribution-sea", 2, 15),
    ("main", "dna-insert", "dna-insert", 2, 16),
    ("main", "feal-differential-cryptanalysis", "feal-differentia", 2, 14),
    ("main", "fix-code-vulnerability", "fix-code-vulnera", 2, 11),
    ("main", "fix-git", "fix-git", 2, 14),
    ("main", "git-leak-recovery", "git-leak-recover", 2, 7),
    ("main", "git-multibranch", "git-multibranch", 2, 12),
    ("main", "headless-terminal", "headless-termina", 2, 14),
    ("calibration", "adaptive-rejection-sampler", "adaptive-rejecti", 2, 19),
    ("calibration", "code-from-image", "code-from-image", 2, 15),
]


def canonical_json(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def visible_messages(steps, target):
    messages = []
    for step in steps:
        if step["step_id"] >= target:
            break
        if step.get("extra", {}).get("context_management"):
            raise ValueError(
                "Explicit handling required for context-management boundaries"
            )
        source, content = step["source"], step["message"]
        if not isinstance(content, str):
            raise TypeError("This replay accepts text history")
        if source in ("user", "system"):
            messages.append({"role": source, "content": content})
        elif source == "agent":
            assistant = {"role": "assistant", "content": content}
            if step.get("tool_calls"):
                assistant["tool_calls"] = [
                    {
                        "id": call["tool_call_id"],
                        "type": "function",
                        "function": {
                            "name": call["function_name"],
                            "arguments": json.loads(canonical_json(call["arguments"])),
                        },
                    }
                    for call in step["tool_calls"]
                ]
            messages.append(assistant)
            for result in step.get("observation", {}).get("results", []):
                tool = {"role": "tool", "content": result["content"]}
                if result.get("source_call_id"):
                    tool["tool_call_id"] = result["source_call_id"]
                messages.append(tool)
        else:
            raise ValueError(f"Unsupported ATIF source: {source}")
    return messages


def prepare(snapshot, tokenizer):
    rows = []
    for partition, task, folder, first, last in SCOPE:
        candidates = [
            snapshot / "trajectories/terminus2" / folder / "agent/trajectory.json",
            snapshot / task / "agent/trajectory.json",
        ]
        path = next((p for p in candidates if p.exists()), None)
        if path is None:
            raise FileNotFoundError(candidates[0])
        trajectory = json.loads(path.read_text(encoding="utf-8"))
        steps = trajectory["steps"]
        by_id = {s["step_id"]: s for s in steps}
        if list(by_id) != sorted(by_id) or len(by_id) != len(steps):
            raise ValueError(f"Unordered or duplicate steps in {task}")
        for step_id in range(first, last + 1):
            if by_id[step_id]["source"] != "agent":
                raise ValueError(
                    f"Selected step is not an agent step: {task}/{step_id}"
                )
            messages = visible_messages(steps, step_id)
            ids = tokenizer.apply_chat_template(
                messages, add_generation_prompt=True, tokenize=True
            )
            rendered = tokenizer.apply_chat_template(
                messages, add_generation_prompt=True, tokenize=False
            )
            if len(ids) + 256 > 32768:
                raise ValueError(f"Context does not fit: {task}/{step_id}")
            rows.append(
                {
                    "partition": partition,
                    "task": task,
                    "step_id": step_id,
                    "request_id": f"{task}:step-{step_id:03d}",
                    "session_id": trajectory["session_id"],
                    "input_tokens": len(ids),
                    "messages": messages,
                    "prompt_token_ids": ids,
                    "prompt_sha256": hashlib.sha256(rendered.encode()).hexdigest(),
                    "token_ids_sha256": hashlib.sha256(
                        canonical_json(ids).encode()
                    ).hexdigest(),
                }
            )
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--tokenizer", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    from transformers import AutoTokenizer

    tokenizer = AutoTokenizer.from_pretrained(args.tokenizer, local_files_only=True)
    rows = prepare(args.snapshot, tokenizer)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="\n") as stream:
        for row in rows:
            stream.write(canonical_json(row) + "\n")
    print(
        json.dumps(
            {
                "revision": REVISION,
                "requests": len(rows),
                "max_input_tokens": max(r["input_tokens"] for r in rows),
            }
        )
    )


if __name__ == "__main__":
    main()
