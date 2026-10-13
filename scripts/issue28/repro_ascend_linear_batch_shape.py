"""Model-free Ascend BF16 linear batch-shape comparison."""

import argparse
import json
import time

import torch
import torch.nn.functional as F
import torch_npu


def compare(a, b):
    a, b = a.cpu(), b.cpu()
    delta = a.float() - b.float()
    return {
        "equal": torch.equal(a, b),
        "different_elements": int((a != b).sum()),
        "max_abs": delta.abs().max().item(),
        "rms": delta.square().mean().sqrt().item(),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=17)
    parser.add_argument("--device", type=int, default=0)
    args = parser.parse_args()
    torch.npu.set_device(args.device)
    torch.manual_seed(args.seed)
    x = (torch.randn(4096, 2048) * 0.1).to(torch.bfloat16).npu()
    weight = (torch.randn(2048, 2048) * 0.02).to(torch.bfloat16).npu()
    started = time.monotonic()
    full = F.linear(x, weight)
    repeated = F.linear(x, weight)
    split = torch.cat([F.linear(part, weight) for part in x.split(2048)])
    padded = torch.cat(
        [
            F.linear(F.pad(part, (0, 0, 0, 2048)), weight)[:2048]
            for part in x.split(2048)
        ]
    )
    result = {
        "torch": torch.__version__,
        "torch_npu": torch_npu.__version__,
        "seed": args.seed,
        "input_shape": list(x.shape),
        "weight_shape": list(weight.shape),
        "dtype": str(x.dtype),
        "cold_repeat": compare(full, repeated),
        "full_vs_split": compare(full, split),
        "full_vs_padded_split": compare(full, padded),
        "elapsed_seconds": time.monotonic() - started,
    }
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
