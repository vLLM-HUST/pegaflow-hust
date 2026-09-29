#!/usr/bin/env bash
set -eo pipefail

if [[ $# -ne 3 ]]; then
  echo "usage: $0 OUTPUT_DIR CONCURRENCY DURATION_SECONDS" >&2
  exit 2
fi

output_dir=$1
concurrency=$2
duration=$3
evidence_dir=$(dirname "$output_dir")
mkdir -p "$evidence_dir"

if [[ -e "$output_dir" ]]; then
  echo "refusing existing output directory: $output_dir" >&2
  exit 2
fi

curl -fsS http://127.0.0.1:33784/health > "$evidence_dir/health-before-c${concurrency}.txt"
curl -fsS http://127.0.0.1:33784/metrics > "$evidence_dir/metrics-before-c${concurrency}.txt"
curl -fsS http://127.0.0.1:9091/metrics > "$evidence_dir/pegaflow-metrics-before-c${concurrency}.txt"
npu-smi info > "$evidence_dir/npu-before-c${concurrency}.txt"
date -u +%FT%TZ > "$evidence_dir/client-start-c${concurrency}.txt"

set +e
/root/frontier-kvcompress-env/bin/swe-prefix-reuse run \
  --workload /root/frontier-kvcompress-runs/workload/qwen35-prepared.json \
  --endpoint http://127.0.0.1:33784/v1/completions \
  --model frontier-qwen35-unified \
  --server-max-context 262144 \
  --concurrency "$concurrency" \
  --duration "$duration" \
  --chips 2 \
  --seed 17 \
  --timeout 1800 \
  --server-metadata /root/pegaflow-qwen35-qualification/server-metadata.json \
  --output "$output_dir" \
  2>&1 | tee "$evidence_dir/client-c${concurrency}.log"
client_status=${PIPESTATUS[0]}
set -e

printf '%s\n' "$client_status" > "$evidence_dir/client-exit-c${concurrency}.txt"
date -u +%FT%TZ > "$evidence_dir/client-end-c${concurrency}.txt"
curl -fsS http://127.0.0.1:33784/metrics > "$evidence_dir/metrics-after-c${concurrency}.txt"
curl -fsS http://127.0.0.1:9091/metrics > "$evidence_dir/pegaflow-metrics-after-c${concurrency}.txt"
npu-smi info > "$evidence_dir/npu-after-c${concurrency}.txt"

if [[ $client_status -ne 0 ]]; then
  exit "$client_status"
fi

python - "$output_dir/summary.json" "$duration" <<'PY'
import json
import sys

summary = json.load(open(sys.argv[1]))
duration = float(sys.argv[2])
assert summary["valid"] is True, summary
assert summary["failed_requests"] == 0, summary
assert summary["measurement_seconds"] == duration, summary
assert summary["planned_measurement_seconds"] == duration, summary
assert summary["aborted"] is False, summary
print(json.dumps(summary, indent=2))
PY

find "$evidence_dir" -type f ! -name SHA256SUMS -print0 \
  | sort -z \
  | xargs -0 sha256sum > "$evidence_dir/SHA256SUMS"
