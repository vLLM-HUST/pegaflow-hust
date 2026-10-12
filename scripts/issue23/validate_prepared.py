"""Check frozen requests using the tokenizer backend independently of rendering."""

import argparse
import hashlib
from pathlib import Path

from tokenizers import Tokenizer

from common import read_json, sha256, write_json


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    root = args.root
    contract = read_json(root / "prepared/contract.json")
    tokenizer = Tokenizer.from_file(str(Path(contract["model_path"]) / "tokenizer.json"))
    report = {"status": "passed", "splits": {}, "hardware_qualification": "pending"}
    ids = set()
    for split, expected in contract["split_summary"].items():
        path = root / f"prepared/{split}.json"
        cases = read_json(path)
        assert sha256(path) == expected["sha256"]
        assert len(cases) == expected["requests"]
        cohorts = {}
        for case in cases:
            encoded = tokenizer.encode(case["prompt"], add_special_tokens=False).ids
            assert encoded == case["input_token_ids"]
            assert len(encoded) == case["input_tokens"] <= contract["max_input_tokens"]
            assert len(encoded) >= contract.get("min_input_tokens", 0)
            assert hashlib.sha256(case["prompt"].encode()).hexdigest() == case["prompt_sha256"]
            assert case["payload"]["prompt"] == case["prompt"]
            assert "<think>\n\n</think>\n\n" in case["prompt"][-100:]
            assert "answer" not in case["payload"] and "reference_answer" not in case["payload"]
            key = (case["dataset"], case["source_id"])
            assert key not in ids
            ids.add(key)
            cohorts.setdefault(case["cohort_id"], []).append(case)
        prefixes = []
        max_length_spread = 0
        for members in cohorts.values():
            assert sorted(m["smooth_slot"] for m in members) == [0, 1, 2, 3]
            assert len({m["category"] for m in members}) == 1
            assert len({m["planned_offset_seconds"] for m in members}) == 1
            assert len({tuple(m["input_token_ids"][:32]) for m in members}) == 1
            if contract.get("runtime_attention_block_tokens"):
                block = contract["runtime_attention_block_tokens"]
                assert len({tuple(m["input_token_ids"][:block]) for m in members}) == 4
            prefixes.append(tuple(members[0]["input_token_ids"][:32]))
            lengths = [m["input_tokens"] for m in members]
            max_length_spread = max(max_length_spread, max(lengths) - min(lengths))
        assert len(set(prefixes)) == len(prefixes)
        report["splits"][split] = {"requests": len(cases), "cohorts": len(cohorts),
                                    "unique_first_32_token_prefixes": len(set(prefixes)),
                                    "max_within_cohort_length_spread": max_length_spread,
                                    "independent_tokenizer_check": "passed"}
    model_check = read_json(root / "checks/model-sha256.json")
    assert model_check["status"] == "passed" and len(model_check["files"]) == 27
    report["unique_source_records"] = len(ids)
    report["contract_sha256"] = sha256(root / "prepared/contract.json")
    write_json(root / "checks/prepared-validation.json", report)
    print(report)


if __name__ == "__main__":
    main()
