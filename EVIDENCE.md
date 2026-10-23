# Canonical historical data

This migration batch is stored only in [vllm-hust-benchmark](https://github.com/vLLM-HUST/vllm-hust-benchmark/tree/2144d58de108cddaff3800fdcbf12dc9d3ab1d18/archive/mod-consumers-20261023/pegaflow).
`BENCHMARK_EVIDENCE.json` pins the manifest and its SHA256. The manifest preserves original
repository/commit/path identities, failures and every file hash. This is custody, not a new result.

```bash
python scripts/fetch_mod_consumer_evidence.py --destination /absolute/new/external/evidence
export BENCHMARK_EVIDENCE_ROOT=/absolute/new/external/evidence
```

The destination must be new and outside this checkout. The downloader verifies the archive and
every member before writing, rejects links/unsafe paths, and never falls back to local copies.
Use `$BENCHMARK_EVIDENCE_ROOT/results/issue23/20261015-contention/` for this batch's original data.
Existing generation commands may still write new local outputs; do not commit them here.

Other historical directories are not declared migrated by this batch. Original failures and
diagnostic/NO_GO scopes remain unchanged. No hardware experiment is required to read this archive.

Run the unchanged contention checks against the downloaded originals:

```bash
python scripts/issue23/verify_contention.py --root "$BENCHMARK_EVIDENCE_ROOT/results/issue23/20261015-contention"
python -m pytest -q scripts/issue23/test_contention_evidence.py
python scripts/issue23/render_contention.py --root "$BENCHMARK_EVIDENCE_ROOT/results/issue23/20261015-contention" --output /absolute/new/figures
```

The corruption regression still changes a checkpoint, recomputes its hash, and requires the
semantic validator to reject it. Archive integrity does not replace that assertion.
