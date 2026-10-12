"""Verify a local checkpoint against the frozen official file tree."""

import argparse
import hashlib
import json
import os
import time
from pathlib import Path


def digest(path, algorithm, prefix=b""):
    value = hashlib.new(algorithm)
    value.update(prefix)
    with path.open("rb") as source:
        while chunk := source.read(8 * 1024 * 1024):
            value.update(chunk)
    return value.hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", required=True, type=Path)
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if hasattr(os, "nice"):
        os.nice(10)
    files = json.loads(args.source.read_text(encoding="utf-8-sig"))
    result = {
        "model": str(args.model),
        "revision": "59d61f3ce65a6d9863b86d2e96597125219dc754",
        "official_tree_sha256": digest(args.source, "sha256"),
        "started_unix": time.time(),
        "files": [],
        "status": "running",
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    for record in files:
        if record["type"] != "file":
            continue
        started = time.monotonic()
        path = args.model / record["path"]
        before = path.stat()
        sha256 = digest(path, "sha256")
        if record.get("lfs"):
            expected = record["lfs"]["oid"]
            actual = sha256
            algorithm = "sha256"
        else:
            expected = record["oid"]
            actual = digest(path, "sha1", f"blob {before.st_size}\0".encode())
            algorithm = "git_blob_sha1"
        after = path.stat()
        item = {
            "path": record["path"], "bytes": after.st_size, "sha256": sha256,
            "expected_algorithm": algorithm, "expected": expected, "actual": actual,
            "size_matches": after.st_size == record["size"],
            "content_matches": actual == expected,
            "stable_during_read": (before.st_size, before.st_mtime_ns)
            == (after.st_size, after.st_mtime_ns),
            "elapsed_seconds": time.monotonic() - started,
        }
        result["files"].append(item)
        args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        print(json.dumps({"checked": len(result["files"]), **item}), flush=True)
    result["finished_unix"] = time.time()
    result["status"] = "passed" if all(
        f["size_matches"] and f["content_matches"] and f["stable_during_read"]
        for f in result["files"]
    ) and len(result["files"]) == 27 else "failed"
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": result["status"], "files": len(result["files"])}), flush=True)
    raise SystemExit(0 if result["status"] == "passed" else 1)


if __name__ == "__main__":
    main()
