"""Bounded startup barrier for the freshly launched, unused owned D controller."""

import argparse
import json
from pathlib import Path
import time
import urllib.error
import urllib.request


def get(url):
    with urllib.request.urlopen(url, timeout=2) as response:
        return json.load(response)


def wait_ready(url, timeout, *, fetch=get, clock=time.monotonic, sleep=time.sleep):
    deadline = clock() + timeout
    while clock() < deadline:
        try:
            state = fetch(url)
        except (OSError, urllib.error.URLError):
            sleep(0.2)
            continue
        if (state.get("ready") is not True or type(state.get("cursor")) is not int
                or state["cursor"] != 0 or state.get("phase") is not None
                or state.get("children") != []):
            raise ValueError("D is not the expected fresh unused phase controller")
        return state
    raise TimeoutError("D control listener did not become ready within startup bound")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=120)
    args = parser.parse_args()
    state = wait_ready(args.url, args.timeout)
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump({"state": state, "ready_unix": time.time(),
                   "timeout_seconds": args.timeout, "formal_sampling_started": False}, stream, indent=2)
        stream.write("\n")
