#!/usr/bin/env bash
set -euo pipefail
# shellcheck source=lib.sh
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)/lib.sh"

mode="${MO_25782_MODE:-fixed}"
case "${mode}" in
    fixed|historical) ;;
    *) die "MO_25782_MODE must be fixed or historical" ;;
esac
fixed_frontend_limits=""
if [[ "${mode}" == fixed ]]; then
    fixed_frontend_limits=$'processLimitationSize = 201326592\nprocessLimitationSpillSize = 1073741824'
fi

runtime_arg="${MO_25782_RUNTIME:-}"
while (($#)); do
    case "$1" in
        --runtime) (($# >= 2)) || die "--runtime requires an argument"; runtime_arg="$2"; shift 2 ;;
        -h|--help) printf 'usage: %s [--runtime ABSOLUTE_PATH]\n' "$0"; exit 0 ;;
        *) die "unknown argument: $1" ;;
    esac
done
if [[ -z "${runtime_arg}" ]]; then
    runtime_arg="/tmp/mo-25782-$(id -u)-$(date +%Y%m%d%H%M%S)-$$"
fi
runtime="$(runtime_path "${runtime_arg}")"
[[ ! -e "${runtime}" && ! -L "${runtime}" ]] || die "runtime already exists; refusing reuse: ${runtime}"
umask 077
mkdir -- "${runtime}"
chmod 0700 -- "${runtime}"
assert_runtime_private "${runtime}"
export MO_25782_RUNTIME="${runtime}"
export MO_25782_LOG_FILE="${runtime}/harness.log"
: >"${MO_25782_LOG_FILE}"
chmod 0600 -- "${MO_25782_LOG_FILE}"

for d in manifest configs logs shared/SHARED log/data log/LOCAL log/cache log/log \
    tn/data tn/LOCAL tn/cache tn/log cn1/data cn1/LOCAL cn1/cache cn1/log \
    cn2/data cn2/LOCAL cn2/cache cn2/log proxy/data proxy/LOCAL proxy/cache proxy/log; do
    mkdir -p -- "${runtime}/${d}"
    chmod 0700 -- "${runtime}/${d}"
done

uuid_log="$(uuid_new)"
uuid_tn="$(uuid_new)"
uuid_cn1="$(uuid_new)"
uuid_cn2="$(uuid_new)"
uuid_proxy="$(uuid_new)"
[[ "${uuid_log}" != "${uuid_tn}" && "${uuid_tn}" != "${uuid_cn1}" && "${uuid_cn1}" != "${uuid_cn2}" && "${uuid_cn2}" != "${uuid_proxy}" ]] || die "UUID collision"

cat >"${runtime}/configs/log.toml" <<EOF
service-type = "LOG"
data-dir = "${runtime}/log/data"

[log]
level = "info"
format = "console"
max-size = 512

[[fileservice]]
name = "LOCAL"
backend = "DISK-V2"
data-dir = "${runtime}/log/LOCAL"

[[fileservice]]
name = "SHARED"
backend = "DISK-V2"
data-dir = "${runtime}/shared/SHARED"

[fileservice.cache]
memory-capacity = "512MB"
disk-capacity = "8GB"
disk-path = "${runtime}/log/cache"

[[fileservice]]
name = "ETL"
backend = "DISK-ETL"

[logservice]
uuid = "${uuid_log}"
deployment-id = 1
data-dir = "${runtime}/log/data/logservice"
service-host = "127.0.0.1"
logservice-address = "127.0.0.1:32001"
logservice-listen-address = "127.0.0.1:32001"
raft-address = "127.0.0.1:32000"
raft-listen-address = "127.0.0.1:32000"
gossip-address = "127.0.0.1:32002"
gossip-listen-address = "127.0.0.1:32002"
gossip-seed-addresses = ["127.0.0.1:32002"]
logdb-disable-prealloc = true

[logservice.BootstrapConfig]
bootstrap-cluster = true
num-of-log-shards = 1
num-of-tn-shards = 1
num-of-log-shard-replicas = 1
init-hakeeper-members = ["131072:${uuid_log}"]

[observability]
host = "127.0.0.1"
enable-metric-to-prom = true
status-port = 17001
disable-trace = true
EOF

write_common_fs() {
    local instance="$1"
    cat <<EOF
[[fileservice]]
name = "LOCAL"
backend = "DISK-V2"
data-dir = "${runtime}/${instance}/LOCAL"

[[fileservice]]
name = "SHARED"
backend = "DISK-V2"
data-dir = "${runtime}/shared/SHARED"

[fileservice.cache]
memory-capacity = "512MB"
disk-capacity = "8GB"
disk-path = "${runtime}/${instance}/cache"

[[fileservice]]
name = "ETL"
backend = "DISK-ETL"
EOF
}

cat >"${runtime}/configs/tn.toml" <<EOF
service-type = "TN"
data-dir = "${runtime}/tn/data"
[log]
level = "info"
format = "console"
max-size = 512
[hakeeper-client]
service-addresses = ["127.0.0.1:32001"]
$(write_common_fs tn)
[tn]
uuid = "${uuid_tn}"
listen-address = "127.0.0.1:19000"
service-address = "127.0.0.1:19000"
service-host = "127.0.0.1"
[tn.logtail-server]
listen-address = "127.0.0.1:19001"
service-address = "127.0.0.1:19001"
[tn.lockservice]
listen-address = "127.0.0.1:19002"
service-address = "127.0.0.1:19002"
[tn.txn.storage]
backend = "TAE"
[observability]
host = "127.0.0.1"
status-port = 17002
enable-metric-to-prom = true
disable-trace = true
EOF

write_cn_config() {
    local name="$1" uuid="$2" base="$3" sql="$4" status="$5"
    cat >"${runtime}/configs/${name}.toml" <<EOF
service-type = "CN"
data-dir = "${runtime}/${name}/data"
[log]
level = "info"
format = "console"
max-size = 512
[hakeeper-client]
service-addresses = ["127.0.0.1:32001"]
$(write_common_fs "${name}")
[cn]
uuid = "${uuid}"
port-base = ${base}
service-host = "127.0.0.1"
[cn.engine]
type = "distributed-tae"
[cn.frontend]
host = "127.0.0.1"
port = ${sql}
proxy-enabled = true
${fixed_frontend_limits}
[observability]
host = "127.0.0.1"
status-port = ${status}
enable-metric-to-prom = true
disable-trace = true
EOF
}
write_cn_config cn1 "${uuid_cn1}" 18100 16001 17003
write_cn_config cn2 "${uuid_cn2}" 18200 16002 17004

cat >"${runtime}/configs/proxy.toml" <<EOF
service-type = "PROXY"
data-dir = "${runtime}/proxy/data"
[log]
level = "info"
format = "console"
max-size = 512
[hakeeper-client]
service-addresses = ["127.0.0.1:32001"]
$(write_common_fs proxy)
[proxy]
uuid = "${uuid_proxy}"
listen-address = "127.0.0.1:6001"
[observability]
host = "127.0.0.1"
status-port = 17005
enable-metric-to-prom = true
disable-trace = true
EOF

ports_all="32000 32001 32002 $(seq -s ' ' 19000 19020) $(seq -s ' ' 18100 18120) 16001 $(seq -s ' ' 18200 18220) 16002 6001 17001 17002 17003 17004 17005 6061 6062 6063 6064 6065"
precheck_ports "${ports_all}"

env_tmp="${runtime}/harness.env.tmp.$$"
{
    printf 'MO_25782_RUNTIME=%q\n' "${runtime}"
    printf 'MO_25782_MODE=%q\n' "${mode}"
    printf 'BINARY=%q\n' "${MO_25782_BINARY:-${REPO_ROOT}/mo-service}"
    printf 'MO_25782_MYSQL_PASSWORD=%q\n' "${MO_25782_MYSQL_PASSWORD:-111}"
    printf 'LOG_UUID=%q\nTN_UUID=%q\nCN1_UUID=%q\nCN2_UUID=%q\nPROXY_UUID=%q\n' "${uuid_log}" "${uuid_tn}" "${uuid_cn1}" "${uuid_cn2}" "${uuid_proxy}"
    printf 'LOG_CONFIG=%q\nTN_CONFIG=%q\nCN1_CONFIG=%q\nCN2_CONFIG=%q\nPROXY_CONFIG=%q\n' \
        "${runtime}/configs/log.toml" "${runtime}/configs/tn.toml" "${runtime}/configs/cn1.toml" "${runtime}/configs/cn2.toml" "${runtime}/configs/proxy.toml"
    printf 'LOG_PORTS=%q\nTN_PORTS=%q\nCN1_PORTS=%q\nCN2_PORTS=%q\n' \
        '32000 32001 32002' "$(seq -s ' ' 19000 19020)" "$(seq -s ' ' 18100 18120) 16001" "$(seq -s ' ' 18200 18220) 16002"
    printf 'CN1_SERVICE_ADDR=%q\nCN2_SERVICE_ADDR=%q\n' '127.0.0.1:18100' '127.0.0.1:18200'
    printf 'PROXY_PORT=6001\nSTATUS_PORTS=%q\nDEBUG_PORTS=%q\nPORTS_ALL=%q\n' \
        '17001 17002 17003 17004 17005' '6061 6062 6063 6064 6065' "${ports_all}"
    printf 'MEMORY_LOG=%q\nMEMORY_TN=%q\nMEMORY_CN1=%q\nMEMORY_CN2=%q\nMEMORY_PROXY=%q\n' '1536M' '2560M' '2560M' '2560M' '512M'
    if cgroup_available; then
        printf 'CGROUP_VERIFIABLE=1\nREPRO_ALLOWED=1\nSMOKE_ONLY=0\n'
    else
        printf 'CGROUP_VERIFIABLE=0\nREPRO_ALLOWED=0\nSMOKE_ONLY=1\n'
    fi
} >"${env_tmp}"
chmod 0600 -- "${env_tmp}"
mv -f -- "${env_tmp}" "${runtime}/harness.env"

printf 'runtime=%s\n' "${runtime}"
printf 'status=%s\n' "${runtime}/harness.env"
