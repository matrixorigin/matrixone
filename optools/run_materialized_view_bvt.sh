#!/usr/bin/env bash
# SQL contract checks reuse the existing multi-CN BVT deployment. Performance
# and restart experiments remain separate, explicitly invoked validation modes.
set -euo pipefail
if [[ $# != 2 ]]; then
    echo "Usage: $0 <repository-root> <report-directory>" >&2
    exit 2
fi
repo_root=$(cd "$1" && pwd)
mkdir -p "$2" "${repo_root}/coverage"
report_dir=$(cd "$2" && pwd)
coverage_dir=$(cd "${repo_root}/coverage" && pwd)
# A rerun must not accept a previous success if the client cannot write JSON.
rm -f "${report_dir}/consumers.json" "${report_dir}/functional.json" "${report_dir}/failure.json" "${report_dir}/multi-cn.json" "${report_dir}/native-probe.json"

cn_dsn() {
    local endpoint port
    endpoint=$(docker compose -f "${repo_root}/etc/launch-tae-compose/compose.yaml" \
        --profile launch-multi-cn port "$1" 6001)
    port=${endpoint##*:}
    [[ "${port}" =~ ^[0-9]+$ ]]
    printf 'dump:111@tcp(127.0.0.1:%s)/?timeout=5s&readTimeout=330s&writeTimeout=330s' "${port}"
}

# Overrides support an existing local deployment without starting more servers.
primary_dsn=${MV_BVT_DSN:-$(cn_dsn cn-0)}
second_dsn=${MV_BVT_SECOND_DSN:-$(cn_dsn cn-1)}
build_dir=$(mktemp -d)
trap 'rm -rf "${build_dir}"' EXIT
cd "${repo_root}"
# Match mo-service's default -cover mode so covdata can combine the producer.
CGO_ENABLED=0 go build -mod=readonly -cover -covermode=set \
    -o "${build_dir}/mv-validation" ./test/materialized_view
CGO_ENABLED=0 go build -mod=readonly -cover -covermode=set \
    -o "${build_dir}/mv-native-probe" ./test/materialized_view/native_probe
export GOCOVERDIR="${coverage_dir}"
# The same JSON bridge drives the destructive native recovery suite. Exercise
# its public stream/session/error contract on this existing deployment too;
# its coverage counters join the unchanged UT/BVT coverage aggregation.
python3 "${repo_root}/test/materialized_view/native_probe/check.py" \
    "${build_dir}/mv-native-probe" "${primary_dsn}" > "${report_dir}/native-probe.json"
"${build_dir}/mv-validation" -dsn "${primary_dsn}" -functional \
    -output "${report_dir}/functional.json"
"${build_dir}/mv-validation" -dsn "${primary_dsn}" -edge-check \
    -output "${report_dir}/failure.json"
"${build_dir}/mv-validation" -dsn "${primary_dsn}" -consumer-check \
    -output "${report_dir}/consumers.json"
"${build_dir}/mv-validation" -dsn "${primary_dsn}" -second-dsn "${second_dsn}" \
    -output "${report_dir}/multi-cn.json"
# Fail closed if a mode exits without its nonempty, successful evidence report.
python3 - "${report_dir}" <<'PY'
import json
import pathlib
import sys

probe = json.loads((pathlib.Path(sys.argv[1]) / "native-probe.json").read_text())
if probe != {"runs": [{"passed_requests": 9, "process_exit": 0}] * 2}:
    raise SystemExit("Incomplete materialized-view native SQL bridge checks")
print("Materialized-view native SQL bridge: 2 x 9 requests passed")

for name in ("functional", "failure", "consumers", "multi-cn"):
    result = json.loads((pathlib.Path(sys.argv[1]) / (name + ".json")).read_text())
    if result.get("error") or not result.get("passed_cases"):
        raise SystemExit("Incomplete materialized-view BVT: " + name)
    print("Materialized-view BVT:", name, len(result["passed_cases"]), "checks passed")
PY
