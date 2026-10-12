"""Re-render the frozen benchmark items for the observed hybrid cache granularity."""
import hashlib
import json
import os
import shutil
import sys
from pathlib import Path

ROOT = Path('/data/shared_upload/hust-zsx/tailguard-issue23-20261012')
sys.path.insert(0, str(ROOT / 'scripts'))
from common import distribution, read_json, sha256, write_json
from prepare_data import render
from transformers import AutoTokenizer

original = ROOT / 'checks/prepared-before-transfer-context'
original.mkdir(exist_ok=False)
for path in (ROOT / 'prepared').glob('*.json'):
    shutil.copy2(path, original / path.name)
contract = read_json(original / 'contract.json')
tokenizer = AutoTokenizer.from_pretrained(contract['model_path'], local_files_only=True)
repeated = {}
for split in contract['split_summary']:
    cases = read_json(original / f'{split}.json')
    for case in cases:
        messages = case['messages'][1:]
        rendered, prompt, ids = render(tokenizer, messages, case['cohort_id'], min_input_tokens=4097)
        again = render(tokenizer, messages, case['cohort_id'], min_input_tokens=4097)
        assert (rendered, prompt, ids) == again
        case.update(messages=rendered, prompt=prompt, input_token_ids=ids, input_tokens=len(ids),
                    prompt_sha256=hashlib.sha256(prompt.encode()).hexdigest())
        case['payload']['prompt'] = prompt
    path = ROOT / f'prepared/{split}.json'
    write_json(path, cases)
    contract['split_summary'][split].update(sha256=sha256(path), input_tokens=distribution([c['input_tokens'] for c in cases]))
    repeated[split] = {'identical_repeat_render': True, 'sha256': sha256(path)}
contract.update(min_input_tokens=4097, runtime_attention_block_tokens=2048,
                context_protocol='original benchmark question and examples followed by a fixed ignorable reference appendix')
write_json(ROOT / 'prepared/contract.json', contract)
write_json(ROOT / 'checks/transfer-context.json', {'status': 'passed', 'previous_contract_sha256': sha256(original / 'contract.json'),
                                                 'contract_sha256': sha256(ROOT / 'prepared/contract.json'),
                                                 'splits': repeated, 'reason': 'Observed attention block size is 2048 tokens.'})
print(json.dumps(contract['split_summary'], indent=2), flush=True)
