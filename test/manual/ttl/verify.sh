#!/usr/bin/env bash
set -euo pipefail
# Run from any directory. Use the repository's configured Go/native toolchain.
ttl_repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
cd "$ttl_repo_root"
ttl_output_dir=${TTL_OUTPUT_DIR:-/tmp/matrixone-ttl-validation}
mkdir -p "$ttl_output_dir"
export CGO_CFLAGS="${CGO_CFLAGS:-} -I$ttl_repo_root/cgo -I$ttl_repo_root/thirdparties/install/include"
export CGO_LDFLAGS="${CGO_LDFLAGS:-} -L$ttl_repo_root/cgo -lmo -L$ttl_repo_root/thirdparties/install/lib -fopenmp"
export LD_LIBRARY_PATH="$ttl_repo_root/lib:$ttl_repo_root/thirdparties/install/lib:${LD_LIBRARY_PATH:-}"
go test -json ./pkg/sql/ttl ./pkg/sql/parsers/dialect/mysql ./pkg/sql/plan -count=1 -timeout=10m > "$ttl_output_dir/unit.jsonl"
go test -json ./pkg/cnservice -run 'Test_registerExecutorsLocked|LineageGCCron' -count=1 > "$ttl_output_dir/registration.jsonl"
go test -json ./pkg/tests/sqlintegration -run '^TestTTL' -count=1 -timeout=12m > "$ttl_output_dir/integration.jsonl"
go test ./pkg/sql/ttl -run '^$' -fuzz '^FuzzTTLConstraintDecode$' -fuzztime="${TTL_FUZZ_TIME:-15s}" -parallel=2 -timeout=5m > "$ttl_output_dir/fuzz.log" 2>&1
go build -o "$ttl_output_dir/mo-service-ttl" ./cmd/mo-service
printf 'TTL validation passed. Results and binary: %s\n' "$ttl_output_dir"
