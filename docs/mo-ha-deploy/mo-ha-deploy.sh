#!/usr/bin/env bash
# =============================================================================
# mo-ha-deploy.sh — MatrixOne 双机主备(DRBD + Pacemaker/STONITH) 一键部署
#
# 支持 RPM 系(RHEL/CentOS/Rocky/AlmaLinux) 与 DEB 系(Debian/Ubuntu)。
# 覆盖：系统准备 -> 依赖安装 -> Docker -> MatrixOne 配置 -> DRBD
#       -> corosync/pacemaker 集群 -> fencing(STONITH) -> 资源 -> 部署后检查
#
# 所有用户相关的值都通过参数或 --config 文件传入，脚本内不写死环境。
# 参数很多时，推荐用 --config 文件（见 deploy.env.example）。
#
# 用法（两台机器都执行，只有一台加 --bootstrap）：
#   node2:  ./mo-ha-deploy.sh --config node2.env --stages pre,pkg,docker,moconf,drbd,cluster --yes
#   node1:  ./mo-ha-deploy.sh --config node1.env --bootstrap --format-drbd --yes
# =============================================================================
set -Eeuo pipefail
CURRENT_STAGE="config"
# 不打印 BASH_COMMAND：配置、认证命令可能含有口令。
trap '_err_rc=$?; printf "ERROR: 阶段 %s 在第 %s 行失败（退出码 %s），停止执行后续阶段。\n" "$CURRENT_STAGE" "$LINENO" "$_err_rc" >&2; exit "$_err_rc"' ERR

# --------------------------- 预扫描 --config --------------------------------
CONFIG_FILE=""
_argv=("$@"); _i=0
while [ $_i -lt ${#_argv[@]} ]; do
  case "${_argv[$_i]}" in
    --config) CONFIG_FILE="${_argv[$((_i+1))]:-}";;
    -h|--help) ;; # 稍后处理
  esac
  _i=$((_i+1))
done
if [ -n "$CONFIG_FILE" ]; then
  [ -r "$CONFIG_FILE" ] || { echo "ERROR: 无法读取 --config 文件: $CONFIG_FILE" >&2; exit 1; }
  # 该文件是被 root source 的（等效 root 执行任意 shell），因此拒绝 group/other 可写：
  # 否则任何能改写它的人都能借本次 sudo 取得 root，而运维通常把它当作纯数据文件。
  _cf_mode=$(stat -c '%a' "$CONFIG_FILE" 2>/dev/null || stat -f '%Lp' "$CONFIG_FILE" 2>/dev/null || true)
  case "$_cf_mode" in
    ''|*[!0-7]*) : ;;
    *) if [ $((8#$_cf_mode & 022)) -ne 0 ]; then
         echo "ERROR: $CONFIG_FILE 权限为 ${_cf_mode}（group/other 可写）。该文件会被 shell source（等效 root 执行），" >&2
         echo "       请 chmod 600 并确保来源可信后重试。" >&2
         exit 1
       fi;;
  esac
  echo "[*] 读取配置 ${CONFIG_FILE}（按 shell 脚本 source 执行，仅使用可信来源）" >&2
  set -a; . "$CONFIG_FILE"; set +a
fi

# ----------------------------- 默认值 ---------------------------------------
CLUSTER="${CLUSTER:-mo}"
STAGES="${STAGES:-pre,pkg,docker,moconf,drbd,cluster,fence,resources,check}"
SELF="${SELF:-}"; PEER="${PEER:-}"; SELF_IP="${SELF_IP:-}"; PEER_IP="${PEER_IP:-}"
DRBD_DEVICE="${DRBD_DEVICE:-}"; DRBD_VOLUME="${DRBD_VOLUME:-/dev/drbd0}"
DRBD_RESOURCE="${DRBD_RESOURCE:-mo}"; DRBD_PORT="${DRBD_PORT:-7789}"
DATA_DIR="${DATA_DIR:-/mnt/mo-data}"
MO_IMAGE="${MO_IMAGE:-matrixorigin/matrixone:latest}"
MO_DIR="${MO_DIR:-/opt/mo-ha}"; MO_PORT="${MO_PORT:-6001}"; MO_HOSTNAME="${MO_HOSTNAME:-mo-ha}"
# MO root 口令：仅用于 check 阶段做一次 SQL 登录验证；留空则跳过 SQL 检查
MO_ROOT_PASSWORD="${MO_ROOT_PASSWORD:-}"
# 对外发布地址：默认对所有网卡开放；生产建议改为内网 IP 或 VIP
MO_BIND_IP="${MO_BIND_IP:-0.0.0.0}"
MO_CONFIG_DIR="${MO_CONFIG_DIR:-}"; NO_MO="${NO_MO:-0}"
DOCKER_MODE="${DOCKER_MODE:-official}"     # official(Docker CE 清华镜像) | distro | skip
# 部署时预拉 MO 镜像：docker-compose RA 的 start 默认超时 240s，
# 首次拉取大镜像(>1GB)极易超时并触发资源迁移，因此默认提前拉取。
PULL_IMAGE="${PULL_IMAGE:-1}"
# p_mo 资源 start 操作超时（秒）：冷启动/首次拉镜慢时放宽，
# 否则 docker-compose RA 默认 240s 超时会让 Pacemaker 判定失败并迁移同组资源。
MO_START_TIMEOUT="${MO_START_TIMEOUT:-600}"
# check 阶段：等待资源稳态、以及等待 MO 可用的上限（秒）
CHECK_SETTLE_SEC="${CHECK_SETTLE_SEC:-180}"; CHECK_MO_WAIT="${CHECK_MO_WAIT:-300}"
DRBD9_QUORUM="${DRBD9_QUORUM:-0}"; FORMAT_DRBD="${FORMAT_DRBD:-0}"
BOOTSTRAP="${BOOTSTRAP:-0}"; DRY_RUN="${DRY_RUN:-0}"; ASSUME_YES="${ASSUME_YES:-0}"
HACLUSTER_PW="${HACLUSTER_PW:-}"; OPEN_PORTS="${OPEN_PORTS:-1}"
VIP="${VIP:-}"
# 对象存储（可选）：设置 S3_BUCKET 即启用
S3_ENDPOINT="${S3_ENDPOINT:-}"; S3_BUCKET="${S3_BUCKET:-}"; S3_REGION="${S3_REGION:-}"
S3_KEY_ID="${S3_KEY_ID:-}"; S3_KEY_SECRET="${S3_KEY_SECRET:-}"; S3_KEY_PREFIX="${S3_KEY_PREFIX:-server/data}"
# fencing
FENCE_SCENARIO="${FENCE_SCENARIO:-}"; FENCE_LOGIN="${FENCE_LOGIN:-}"; FENCE_PASSWORD="${FENCE_PASSWORD:-}"
FENCE_SNMP="${FENCE_SNMP:-public}"
FENCE_TARGET_SELF="${FENCE_TARGET_SELF:-}"; FENCE_TARGET_PEER="${FENCE_TARGET_PEER:-}"
FENCE_PLUG_SELF="${FENCE_PLUG_SELF:-}"; FENCE_PLUG_PEER="${FENCE_PLUG_PEER:-}"
FENCE_SHARED_IP="${FENCE_SHARED_IP:-}"; FENCE_EXTRA="${FENCE_EXTRA:-}"
FENCE_DELAY_MAX="${FENCE_DELAY_MAX:-5}"; FENCE_AGENT_OVERRIDE="${FENCE_AGENT_OVERRIDE:-}"
FENCE_ADDR_PARAM="${FENCE_ADDR_PARAM:-}"   # ipmi/redfish 默认 ipaddr；custom_http 默认 ip
STONITH_TIMEOUT="${STONITH_TIMEOUT:-60}"; FENCE_MONITOR_TIMEOUT="${FENCE_MONITOR_TIMEOUT:-60}"
DRBD_HELPER_DIR="${DRBD_HELPER_DIR:-}"    # 留空时查找 /usr/lib/drbd、/usr/lib64/drbd
SBD_DEVICE="${SBD_DEVICE:-}"; WATCHDOG="${WATCHDOG:-/dev/watchdog}"
# DRBD 内核模块：ELRepo 无匹配包时(如 aarch64/RHEL9)允许从源码构建
DRBD_SRC_URL="${DRBD_SRC_URL:-https://pkg.linbit.com//downloads/drbd/9/drbd-9.2.20.tar.gz}"
DRBD_KERNEL_FALLBACK="${DRBD_KERNEL_FALLBACK:-auto}"   # auto | never

# 脚本目录（用于定位随包下发的 OCF agent）
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AGENT_DIR="${AGENT_DIR:-$SCRIPT_DIR/agents}"

# ----------------------------- 工具函数 -------------------------------------
PASS=0; FAIL=0; WARN=0; CHECK_RC=0
if [ -t 1 ]; then G=$'\033[32m'; R=$'\033[31m'; Y=$'\033[33m'; C=$'\033[36m'; N=$'\033[0m'; else G=; R=; Y=; C=; N=; fi
info(){ printf "${C}[*]${N} %s\n" "$*"; }
ok(){   printf "  ${G}[PASS]${N} %s\n" "$*"; PASS=$((PASS+1)); }
bad(){  printf "  ${R}[FAIL]${N} %s\n" "$*"; FAIL=$((FAIL+1)); }
wrn(){  printf "  ${Y}[WARN]${N} %s\n" "$*"; WARN=$((WARN+1)); }
hdr(){  printf "\n${C}== %s ==${N}\n" "$*"; }
die(){  printf "${R}ERROR:${N} %s\n" "$*" >&2; exit 1; }

# 打印命令时遮蔽凭据：passwd/password='x'、-p 'x'、hacluster:x、snmp_community=x
mask_secrets(){
  printf '%s' "$*" | sed -E \
    -e "s/(passwd|password)='[^']*'/\1='***'/g" \
    -e "s/-p '[^']*'/-p '***'/g" \
    -e 's/(hacluster:)[^ '\''"]*/\1***/g' \
    -e 's/(snmp_community=)[^ ]*/\1***/g'
}
run(){  if [ "$DRY_RUN" = 1 ]; then printf "    (dry-run) %s\n" "$(mask_secrets "$*")"; else info "exec: $(mask_secrets "$*")"; bash -Eeuo pipefail -c "$*"; fi; }
runq(){ if [ "$DRY_RUN" = 1 ]; then printf "    (dry-run) %s\n" "$(mask_secrets "$*")"; else bash -Eeuo pipefail -c "$*"; fi; }
stage_enabled(){ case ",$STAGES," in *",$1,"*) return 0;; *) return 1;; esac; }
validate_stages(){
  local _s
  [ -n "$STAGES" ] || die "--stages 不能为空（可选：pre,pkg,docker,moconf,drbd,cluster,fence,resources,check,teardown）"
  case ",$STAGES," in *",,"*) die "--stages 含空项：\"$STAGES\"" ;; esac
  for _s in ${STAGES//,/ }; do
    case "$_s" in
      pre|pkg|docker|moconf|drbd|cluster|fence|resources|check|teardown) ;;
      *) die "未知阶段 \"$_s\"（可选：pre,pkg,docker,moconf,drbd,cluster,fence,resources,check,teardown）" ;;
    esac
  done
}

usage(){
  cat <<'EOF'
mo-ha-deploy.sh — MatrixOne 双机主备(DRBD+Pacemaker/STONITH) 一键部署

【必需】
  --self NAME            本机节点名(须与对端 /etc/hosts 一致)
  --peer NAME            对端节点名
  --self-ip IP           本机 IP
  --peer-ip IP           对端 IP
  --drbd-device /dev/xx  供 DRBD 使用的裸块设备(两台各自一块盘)
  --fence-scenario S     fencing 场景(见下)

【路径/网络(可选)】
  --cluster NAME         集群名(默认 mo)
  --drbd-resource NAME   DRBD 资源名(默认 mo)
  --drbd-volume DEV      DRBD 逻辑卷(默认 /dev/drbd0)
  --drbd-port PORT       DRBD 端口(默认 7789)
  --data-dir PATH        数据目录(默认 /mnt/mo-data)
  --mo-image IMAGE       MatrixOne 镜像(默认 matrixorigin/matrixone:latest)
  --mo-port PORT         MO 客户端端口(默认 6001)
  --mo-bind-ip IP        MO 端口绑定的本机地址(默认 0.0.0.0，生产建议填内网 IP/VIP)
  --mo-dir PATH          MO 运行目录(默认 /opt/mo-ha)
  --mo-hostname NAME     explicit-hostname(默认 mo-ha，两台必须一致)
  --mo-root-password P   MO root 口令(仅用于 check 阶段验证 SQL 可登录；默认跳过)
  --mo-config-dir PATH   配置载荷目录(默认脚本同级 mo-config/)
  --no-mo                只部署集群存储，不创建 MO 资源
  --vip CIDR             资源虚拟 IP，如 10.0.0.100/24(可选)

【对象存储(可选，设置 --s3-bucket 即启用)】
  --s3-endpoint URL --s3-bucket NAME --s3-region R
  --s3-key-id K --s3-key-secret S --s3-key-prefix P(默认 server/data)

【fencing 场景参数】
  --fence-scenario ipmi|redfish|pdu|vmware|proxmox|virsh|aws|azure|gce|aliyun|custom_http|sbd
  --fence-target-self X  ipmi/redfish/custom_http: 本机带外地址
  --fence-target-peer X  同上: 对端带外地址
  --fence-shared-ip X    pdu/vmware/proxmox/virsh: 共享入口 IP
  --fence-plug-self ID   map 类场景: 本机 plug(VM UUID/插座号/实例ID...)
  --fence-plug-peer ID   同上: 对端 plug
  --fence-login U --fence-password P --fence-snmp COMM
  --fence-extra "k=v,k=v" --fence-delay-max N(默认 5；fence-extra 值不会在日志中遮蔽，勿放敏感值)
  --fence-agent NAME     custom_http 场景的 agent 名
  --fence-addr-param KEY 带外地址参数名（默认 ipmi/redfish=ipaddr, custom_http=ip）
  --sbd-device /dev/sdX --watchdog /dev/watchdog

【行为控制】
  --drbd-src-url URL     DRBD 源码包(默认 LINBIT 9.2.20；内核模块包缺失时用于源码构建)
  --no-drbd-kernel-build 禁止源码构建 DRBD 内核模块(只用发行版/ELRepo 包)
  --config FILE          从文件读取上述变量(CLI 覆盖文件)。注意：该文件被 shell source，
                          内容会被 root 执行，须 chmod 600 且仅用可信来源；group/other 可写会被拒绝
  --bootstrap            引导节点(建集群/fencing/资源)；仅一台使用
  --format-drbd          首次格式化 DRBD(仅 bootstrap 一次)
  --drbd9-quorum         使用 DRBD9 quorum majority(需 kmod-drbd 9.x)
  --docker-mode M        official(默认，Docker CE 清华镜像)|distro|skip
  --no-pull-image        不在部署时预拉 MO 镜像(默认先 docker pull，避免启动超时)
  --hacluster-password P pcsd hacluster 密码(必填，两台须一致)
  --stages LIST          pre,pkg,docker,moconf,drbd,cluster,fence,resources,check,teardown
  --teardown             等价于 --stages teardown(销毁集群/DRBD/MO，危险)
  --no-open-ports        不自动配置防火墙
  --dry-run              只打印不执行
  --yes                  免交互(teardown 等破坏性阶段必需)
EOF
}

# ----------------------------- 参数解析 -------------------------------------
while [ $# -gt 0 ]; do
  case "$1" in
    --config) CONFIG_FILE="${2:-}"; shift 2;;
    --self) SELF="${2:-}"; shift 2;;
    --peer) PEER="${2:-}"; shift 2;;
    --self-ip) SELF_IP="${2:-}"; shift 2;;
    --peer-ip) PEER_IP="${2:-}"; shift 2;;
    --cluster) CLUSTER="${2:-}"; shift 2;;
    --drbd-device) DRBD_DEVICE="${2:-}"; shift 2;;
    --drbd-volume) DRBD_VOLUME="${2:-}"; shift 2;;
    --drbd-resource) DRBD_RESOURCE="${2:-}"; shift 2;;
    --drbd-port) DRBD_PORT="${2:-}"; shift 2;;
    --data-dir) DATA_DIR="${2:-}"; shift 2;;
    --mo-image) MO_IMAGE="${2:-}"; shift 2;;
    --mo-port) MO_PORT="${2:-}"; shift 2;;
    --mo-bind-ip) MO_BIND_IP="${2:-}"; shift 2;;
    --mo-dir) MO_DIR="${2:-}"; shift 2;;
    --mo-hostname) MO_HOSTNAME="${2:-}"; shift 2;;
    --mo-root-password) MO_ROOT_PASSWORD="${2:-}"; shift 2;;
    --mo-config-dir) MO_CONFIG_DIR="${2:-}"; shift 2;;
    --no-mo) NO_MO=1; shift;;
    --vip) VIP="${2:-}"; shift 2;;
    --s3-endpoint) S3_ENDPOINT="${2:-}"; shift 2;;
    --s3-bucket) S3_BUCKET="${2:-}"; shift 2;;
    --s3-region) S3_REGION="${2:-}"; shift 2;;
    --s3-key-id) S3_KEY_ID="${2:-}"; shift 2;;
    --s3-key-secret) S3_KEY_SECRET="${2:-}"; shift 2;;
    --s3-key-prefix) S3_KEY_PREFIX="${2:-}"; shift 2;;
    --fence-scenario) FENCE_SCENARIO="${2:-}"; shift 2;;
    --fence-target-self) FENCE_TARGET_SELF="${2:-}"; shift 2;;
    --fence-target-peer) FENCE_TARGET_PEER="${2:-}"; shift 2;;
    --fence-shared-ip) FENCE_SHARED_IP="${2:-}"; shift 2;;
    --fence-plug-self) FENCE_PLUG_SELF="${2:-}"; shift 2;;
    --fence-plug-peer) FENCE_PLUG_PEER="${2:-}"; shift 2;;
    --fence-login) FENCE_LOGIN="${2:-}"; shift 2;;
    --fence-password) FENCE_PASSWORD="${2:-}"; shift 2;;
    --fence-snmp) FENCE_SNMP="${2:-}"; shift 2;;
    --fence-extra) FENCE_EXTRA="${2:-}"; shift 2;;
    --fence-delay-max) FENCE_DELAY_MAX="${2:-}"; shift 2;;
    --fence-agent) FENCE_AGENT_OVERRIDE="${2:-}"; shift 2;;
    --fence-addr-param) FENCE_ADDR_PARAM="${2:-}"; shift 2;;
    --sbd-device) SBD_DEVICE="${2:-}"; shift 2;;
    --watchdog) WATCHDOG="${2:-}"; shift 2;;
    --drbd-src-url) DRBD_SRC_URL="${2:-}"; shift 2;;
    --no-drbd-kernel-build) DRBD_KERNEL_FALLBACK=never; shift;;
    --hacluster-password) HACLUSTER_PW="${2:-}"; shift 2;;
    --docker-mode) DOCKER_MODE="${2:-}"; shift 2;;
    --no-pull-image) PULL_IMAGE=0; shift;;
    --stages) STAGES="${2:-}"; shift 2;;
    --teardown) STAGES="teardown"; shift;;
    --bootstrap) BOOTSTRAP=1; shift;;
    --format-drbd) FORMAT_DRBD=1; shift;;
    --drbd9-quorum) DRBD9_QUORUM=1; shift;;
    --no-open-ports) OPEN_PORTS=0; shift;;
    --dry-run) DRY_RUN=1; shift;;
    --yes) ASSUME_YES=1; shift;;
    -h|--help) usage; exit 0;;
    *) die "未知参数: $1 (--help 查看用法)";;
  esac
done

# ----------------------------- 凭据校验 -------------------------------------
# 凭据会经 bash -c 拼入命令，含引号/空白/;|& 等元字符会被错误解析，提前拦下。
check_secret(){
  local name="$1" val="$2"
  [ -n "$val" ] || return 0
  case "$val" in
    *[!A-Za-z0-9._@%+=,:/_-]*)
      die "$name 含 shell 元字符(引号/空白/\$ 等)，会破坏部署命令；请改用字母/数字/._@%+=,:/_- 组合" ;;
  esac
}
check_secret "--fence-password" "$FENCE_PASSWORD"
check_secret "--hacluster-password" "$HACLUSTER_PW"

# ----------------------------- pcs 代理绕过 ---------------------------------
# pcs 客户端通过 https://<node>:2224 访问 pcsd；若环境设置了 HTTP(S)_PROXY，
# pcs 会错误地走代理导致 "Unable to communicate with <node>"。
# 用导出函数让所有 pcs 调用（含 runq 的 bash -c 子 shell）都自动绕过代理。
pcs(){
  env -u http_proxy -u https_proxy -u HTTP_PROXY -u HTTPS_PROXY -u all_proxy -u ALL_PROXY \
    no_proxy="localhost,127.0.0.1,${SELF:-},${PEER:-},${SELF_IP:-},${PEER_IP:-}" \
    NO_PROXY="localhost,127.0.0.1,${SELF:-},${PEER:-},${SELF_IP:-},${PEER_IP:-}" \
    pcs "$@"
}
export -f pcs

# ----------------------------- 系统检测 -------------------------------------
OS_FAMILY=""; PKG=""
detect_os(){
  [ -r /etc/os-release ] || die "无法读取 /etc/os-release"
  . /etc/os-release
  if command -v dnf >/dev/null 2>&1; then PKG=dnf
  elif command -v yum >/dev/null 2>&1; then PKG=yum
  elif command -v apt-get >/dev/null 2>&1; then PKG=apt-get
  else die "不支持的包管理器(需要 dnf/yum/apt-get)"; fi
  case "$PKG" in dnf|yum) OS_FAMILY="rpm";; apt-get) OS_FAMILY="deb";; esac
  info "检测到发行版: ${PRETTY_NAME:-${ID:-?}}  包管理器=$PKG  系列=$OS_FAMILY"
}

pkg_update(){ case "$OS_FAMILY" in rpm) runq "$PKG makecache -y";; deb) runq "$PKG update -y";; esac; }
pkg_install(){
  local pkgs="$*"; [ -n "$pkgs" ] || return 0
  case "$OS_FAMILY" in
    rpm) run "$PKG install -y $pkgs";;
    deb) pkg_update || return $?; run "DEBIAN_FRONTEND=noninteractive $PKG install -y $pkgs";;
  esac
}

# EL8/EL9: pacemaker/pcs/resource-agents 位于 highavailability 仓库，默认禁用
enable_ha_repo(){
  [ "$OS_FAMILY" = rpm ] || return 0
  runq "$PKG install -y dnf-plugins-core"
  runq "$PKG config-manager --set-enabled highavailability 2>/dev/null || \
        sed -i '/^\[highavailability\]/,/^\[/ s/^enabled=0/enabled=1/' /etc/yum.repos.d/*.repo"
}

# 把随包下发的 OCF agent 安装到系统(如 EL9 缺失的 heartbeat/docker-compose)
ensure_ocf_agents(){
  local src="$AGENT_DIR/heartbeat"
  [ -d "$src" ] || return 0
  run "install -d -m 0755 /usr/lib/ocf/resource.d/heartbeat"
  local f n
  for f in "$src"/*; do
    [ -f "$f" ] || continue
    n="$(basename "$f")"
    if [ -f "/usr/lib/ocf/resource.d/heartbeat/$n" ]; then
      info "OCF agent 已存在: heartbeat/$n"
    else
      run "install -m 0755 '$f' /usr/lib/ocf/resource.d/heartbeat/$n"
    fi
  done
}

# 从源码构建 DRBD 内核模块(用于 ELRepo 无 aarch64 kmod-drbd 的场景)
build_drbd_from_source(){
  local url="$DRBD_SRC_URL" tb d
  info "从源码构建 DRBD 内核模块: $url"
  pkg_install "gcc make patch tar"
  case "$OS_FAMILY" in
    rpm) pkg_install "kernel-devel-\$(uname -r) kernel-headers elfutils-libelf-devel" \
           || pkg_install "kernel-devel kernel-headers elfutils-libelf-devel";;
    deb) pkg_install "linux-headers-\$(uname -r) build-essential";;
  esac
  case "$url" in
    http*|ftp*) tb="/tmp/$(basename "$url")"
      run "curl -fL --retry 3 -o '$tb' '$url' || wget -O '$tb' '$url'";;
    *) tb="$url"; [ -r "$tb" ] || { wrn "找不到 DRBD 源码包: $tb"; return 1; };;
  esac
  run "tar -C /usr/src -xf '$tb'"
  d="$(basename "$tb")"
  d="${d%.tar.gz}"; d="${d%.tgz}"; d="${d%.tar.bz2}"; d="${d%.tar.xz}"; d="${d%.tar}"
  if [ "$DRY_RUN" != 1 ]; then
    if [ ! -d "/usr/src/$d" ]; then
      # 后缀非 .tar.gz 或顶层目录名与包名不一致时，用 tar 的顶层目录名
      local top; top="$(tar -tf "$tb" | sed -n '1{s#^\./##; s#/.*##; p;}')"
      [ -n "$top" ] && [ -d "/usr/src/$top" ] && d="$top"
    fi
    [ -d "/usr/src/$d" ] || { wrn "DRBD 源码解包目录不存在: /usr/src/$d"; return 1; }
  fi
  run "cd /usr/src/$d && make -j\$(nproc) && make install && depmod -a"
}

# 确保 rpm 系有可加载的 drbd 内核模块
ensure_drbd_module_rpm(){
  lsmod 2>/dev/null | grep -q '^drbd' && { info "DRBD 内核模块已加载"; return 0; }
  if runq "modprobe drbd"; then info "DRBD 内核模块已加载"; return 0; fi
  if [ "$DRY_RUN" = 1 ]; then info "(dry-run) 尝试 ELRepo kmod-drbd / 源码构建"; return 0; fi
  info "本机无 drbd 模块，尝试 ELRepo kmod-drbd ..."
  runq "$PKG install -y https://www.elrepo.org/elrepo-release-\$(rpm -E %rhel).elrepo.noarch.rpm || true"
  runq "$PKG install -y kmod-drbd9x || $PKG install -y kmod-drbd || true"
  if runq "modprobe drbd"; then info "ELRepo DRBD 内核模块安装成功"; return 0; fi
  if [ "$DRBD_KERNEL_FALLBACK" = never ]; then
    wrn "DRBD 内核模块不可用(已用 --no-drbd-kernel-build 禁用源码构建)"
    return 1
  fi
  wrn "ELRepo 无匹配内核模块(常见于 aarch64)，改用源码构建"
  build_drbd_from_source
  runq "modprobe drbd"
  lsmod 2>/dev/null | grep -q '^drbd'
}

# ----------------------------- 阶段 pre -------------------------------------
stage_pre(){
  hdr "阶段 pre: 系统准备"
  [ "$(id -u)" = 0 ] || [ "$DRY_RUN" = 1 ] || die "请以 root 运行"
  require_nodes
  [ -n "$DRBD_DEVICE" ] || die "缺少 --drbd-device"
  command -v hostnamectl >/dev/null 2>&1 && run "hostnamectl set-hostname $SELF"
  strip_hosts_block
  run "printf '# MO-HA-BEGIN\n$SELF_IP $SELF\n$PEER_IP $PEER\n# MO-HA-END\n' >> /etc/hosts"
  run "printf 'net.ipv4.ip_nonlocal_bind=1\nvm.swappiness=10\n' > /etc/sysctl.d/90-mo-ha.conf"
  run "sysctl --system"
  if command -v chronyc >/dev/null 2>&1; then
    runq "systemctl enable --now chronyd 2>/dev/null || systemctl enable --now chrony"
  fi
  info "主机名与 hosts 就绪: $SELF ($SELF_IP) <-> $PEER ($PEER_IP)"
}

# ----------------------------- 阶段 pkg -------------------------------------
stage_pkg(){
  hdr "阶段 pkg: 安装依赖"
  pkg_update
  case "$OS_FAMILY" in
    rpm)
      enable_ha_repo
      runq "$PKG install -y epel-release"
      pkg_install "drbd-utils drbd-pacemaker pacemaker corosync pcs resource-agents fence-agents-all sbd"
      ensure_drbd_module_rpm
      ;;
    deb)
      pkg_install "drbd-utils pacemaker corosync pcs fence-agents resource-agents-base resource-agents-common resource-agents-extra sbd"
      # drbd 内核模块在 linux-modules-extra，必须与当前运行内核版本匹配
      if [ "$DRY_RUN" = 1 ] || ! runq "modprobe drbd"; then
        runq "DEBIAN_FRONTEND=noninteractive apt-get install -y linux-modules-extra-\$(uname -r)"
        runq "depmod -a"
        runq "modprobe drbd"
      fi
      ;;
  esac
  ensure_ocf_agents
  # 两台分别执行 pkg 阶段时都准备代理，故障后任一存活节点均可执行隔离。
  prepare_proxmox_fence
  run "printf 'drbd\n' > /etc/modules-load.d/drbd.conf"
  runq "modprobe drbd"
  if [ "$DRY_RUN" != 1 ] && ! lsmod 2>/dev/null | grep -q '^drbd'; then
    die "DRBD 内核模块仍未加载：请安装与 \$(uname -r) 匹配的内核模块包后重跑 pkg 阶段"
  fi
  if [ "$DRY_RUN" != 1 ]; then drbdadm --version | sed 's/^/    /'; fi
}

# ----------------------------- 阶段 docker ----------------------------------
compose_wrapper_path(){
  # 放 /usr/local/bin，避免覆盖发行版/第三方提供的 /usr/bin/docker-compose。
  # ocf:heartbeat:docker-compose 的 binpath 由本脚本显式指定，路径可自选。
  echo "/usr/local/bin/docker-compose"
}
stage_docker(){
  hdr "阶段 docker: 安装 Docker 与 compose"
  case "$DOCKER_MODE" in
    skip) info "跳过 Docker 安装";;
    distro)
      case "$OS_FAMILY" in
        rpm) pkg_install "docker";;
        deb) pkg_install "docker.io docker-compose-v2";;
      esac;;
    official)
      info "使用 Docker CE 国内镜像源：清华 TUNA"
      case "$OS_FAMILY" in
        rpm)
          runq "$PKG install -y dnf-plugins-core"
          runq "$PKG config-manager --add-repo https://mirrors.tuna.tsinghua.edu.cn/docker-ce/linux/centos/docker-ce.repo"
          # repo 文件中的软件包地址和 GPG 地址也需要替换。
          runq "sed -i 's#https://download.docker.com#https://mirrors.tuna.tsinghua.edu.cn/docker-ce#g' /etc/yum.repos.d/docker-ce.repo"
          pkg_install "docker-ce docker-ce-cli containerd.io docker-compose-plugin";;
        deb)
          pkg_install "ca-certificates curl gnupg"
          runq "install -m0755 -d /etc/apt/keyrings"
          # 下载、转换全部成功后才替换 keyring，失败不留下 0 字节文件。
          runq "key_dir=\$(mktemp -d)
trap 'rm -rf -- \"\$key_dir\"' EXIT
curl -fsSL https://mirrors.tuna.tsinghua.edu.cn/docker-ce/linux/\$(. /etc/os-release; echo \$ID)/gpg -o \"\$key_dir/docker.asc\"
gpg --batch --yes --dearmor -o \"\$key_dir/docker.gpg\" \"\$key_dir/docker.asc\"
test -s \"\$key_dir/docker.gpg\"
install -m 0644 \"\$key_dir/docker.gpg\" /etc/apt/keyrings/docker.gpg"
          runq "echo \"deb [arch=\$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://mirrors.tuna.tsinghua.edu.cn/docker-ce/linux/\$(. /etc/os-release; echo \$ID) \$(. /etc/os-release; echo \$VERSION_CODENAME) stable\" > /etc/apt/sources.list.d/docker.list"
          pkg_install "docker-ce docker-ce-cli containerd.io docker-compose-plugin";;
      esac;;
    *) die "--docker-mode 只能是 official|distro|skip";;
  esac
  if [ "$DRY_RUN" != 1 ]; then command -v docker >/dev/null 2>&1 || die "Docker 未安装，停止 docker 阶段"; fi
  runq "systemctl enable --now docker"
  runq "docker info >/dev/null"
  runq "/usr/bin/docker compose version >/dev/null"
  # compose wrapper：仅在 docker compose 插件确实可用时创建，避免生成不可用的 shim
  local wpath; wpath="$(compose_wrapper_path)"
  if [ "$DRY_RUN" != 1 ] && [ -e "$wpath" ] && ! grep -q 'docker compose' "$wpath" 2>/dev/null; then
    info "$wpath 已存在且非本脚本生成，保留并验证原文件"
  else
      run "mkdir -p $(dirname "$wpath")
cat > $wpath <<'WRAP'
#!/bin/sh
exec /usr/bin/docker compose \"\$@\"
WRAP
chmod 0755 $wpath"
  fi
  runq "'$wpath' version >/dev/null"
  run "mkdir -p $DATA_DIR $MO_DIR/conf"
  # 预拉 MO 镜像（避免 docker-compose RA start 超时触发资源迁移）
  if [ "$NO_MO" != 1 ] && [ "$PULL_IMAGE" = 1 ]; then
    info "预拉取 MO 镜像: ${MO_IMAGE}（首次较慢，可用 --no-pull-image 跳过）"
    runq "docker pull $MO_IMAGE" || die "镜像预拉取失败，停止 docker 阶段；请确认网络/代理，或用 --no-pull-image 显式跳过"
    info "镜像已就绪: $MO_IMAGE"
  fi
  if command -v getenforce >/dev/null 2>&1 && [ "$(getenforce 2>/dev/null)" != "Disabled" ]; then
    runq "semanage fcontext -a -t container_file_t '$DATA_DIR(/.*)?' 2>/dev/null || semanage fcontext -m -t container_file_t '$DATA_DIR(/.*)?'"
    runq "restorecon -R '$DATA_DIR'"
  fi
}

# ----------------------------- 阶段 moconf ----------------------------------
# TOML 基本字符串转义：\ 与 " 不转义会生成非法 toml
toml_escape(){ printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g'; }

build_shared_block(){
  if [ -n "$S3_BUCKET" ]; then
    cat <<S3
[[fileservice]]
name = "SHARED"
backend = "S3"

[fileservice.s3]
endpoint     = "$(toml_escape "$S3_ENDPOINT")"
bucket       = "$(toml_escape "$S3_BUCKET")"
region       = "$(toml_escape "$S3_REGION")"
key-prefix   = "$(toml_escape "$S3_KEY_PREFIX")"
key-id       = "$(toml_escape "$S3_KEY_ID")"
key-secret   = "$(toml_escape "$S3_KEY_SECRET")"
S3
  else
    cat <<'LOCAL'
[[fileservice]]
name = "SHARED"
backend = "DISK-V2"
data-dir = "/mo-data/shared2"
LOCAL
  fi
}
stage_moconf(){
  hdr "阶段 moconf: 下发 MatrixOne 配置"
  if [ "$NO_MO" = 1 ]; then info "NO_MO=1，跳过"; return 0; fi
  # S3 一旦启用就必须给全参数，否则会生成空 endpoint / 空凭据的配置
  if [ -n "$S3_BUCKET" ]; then
    local _miss=""
    [ -n "$S3_ENDPOINT" ]   || _miss="$_miss S3_ENDPOINT"
    [ -n "$S3_KEY_ID" ]     || _miss="$_miss S3_KEY_ID"
    [ -n "$S3_KEY_SECRET" ] || _miss="$_miss S3_KEY_SECRET"
    [ -z "$_miss" ] || die "启用 S3 (S3_BUCKET=$S3_BUCKET) 时必须同时设置：$_miss"
    local _s3v="$S3_ENDPOINT$S3_BUCKET$S3_REGION$S3_KEY_PREFIX$S3_KEY_ID$S3_KEY_SECRET"
    case "$_s3v" in
      *$'\n'*|*$'\r'*) die "S3 参数不能包含换行字符" ;;
    esac
  fi
  local payload="$MO_CONFIG_DIR"
  [ -n "$payload" ] || payload="$(cd "$(dirname "$0")" && pwd)/mo-config"
  if [ ! -d "$payload" ]; then
    die "未找到配置目录 $payload，请用 --mo-config-dir 指定完整配置载荷"
  fi
  local required_file
  for required_file in docker-compose.yaml conf/launch.toml conf/log.toml conf/tn.toml conf/cn.toml; do
    [ -r "$payload/$required_file" ] || die "配置载荷缺失或不可读：$payload/$required_file"
  done
  run "mkdir -p $MO_DIR/conf"
  run "cp -a '$payload/.' $MO_DIR/"

  if [ "$DRY_RUN" = 1 ]; then
    printf "    (dry-run) 替换 compose 占位符 + 生成 SHARED fileservice(%s)\n" \
      "$([ -n "$S3_BUCKET" ] && echo S3 || echo DISK-V2)"
    return 0
  fi
  sed -i "s#__MO_IMAGE__#${MO_IMAGE}#g; s#__MO_PORT__#${MO_PORT}#g; s#__MO_BIND_IP__#${MO_BIND_IP}#g; s#__DATA_DIR__#${DATA_DIR}#g; s#__MO_DIR__#${MO_DIR}#g; s#__MO_HOSTNAME__#${MO_HOSTNAME}#g" "$MO_DIR/docker-compose.yaml"
  sed -i "s#__MO_HOSTNAME__#${MO_HOSTNAME}#g" "$MO_DIR/conf/log.toml"
  # 两个模板都必须不再残留占位符
  if [ "$DRY_RUN" != 1 ] && grep -q '__MO_[A-Z_]*__' "$MO_DIR/docker-compose.yaml" "$MO_DIR/conf/"*.toml 2>/dev/null; then
    die "配置模板仍有未替换的占位符：$(grep -ho '__MO_[A-Z_]*__' "$MO_DIR/docker-compose.yaml" "$MO_DIR/conf/"*.toml | sort -u | tr '\n' ' ')"
  fi
  local blk; blk="$(mktemp)"; build_shared_block > "$blk"
  for f in "$MO_DIR/conf/tn.toml" "$MO_DIR/conf/cn.toml"; do
    awk -v blkfile="$blk" '
      /__SHARED_FILESERVICE__/ { while ((getline l < blkfile) > 0) print l; next } { print }
    ' "$f" > "$f.tmp"
    mv "$f.tmp" "$f"
  done
  rm -f "$blk"
  # S3 凭据是明文写入 conf，收紧权限避免同机其他用户可读
  if [ -n "$S3_BUCKET" ]; then
    runq "chmod 600 $MO_DIR/conf/*.toml"
  fi
  [ -f "$MO_DIR/conf/launch.toml" ] || die "$MO_DIR/conf/launch.toml 缺失"
  ok "已下发 $MO_DIR (compose + conf)"
}

# ----------------------------- 阶段 drbd ------------------------------------
# 节点身份校验：预阶段与 DRBD/集群阶段都要拦，避免 --stages drbd 单独跑时绕过。
# 同名/同 IP 会生成只有单个 host 段的 DRBD 配置且不报错。
# 只删除成对的 MO-HA 标记块。缺 END 时绝不能用 sed 范围地址：GNU sed 会从 BEGIN
# 一直删到文件末尾，把 /etc/hosts 里其它条目一起抹掉（stage_pre / stage_teardown 共用）。
strip_hosts_block(){
  [ -f /etc/hosts ] || return 0
  local b e
  b="$(grep -c '^# MO-HA-BEGIN$' /etc/hosts 2>/dev/null || true)"
  e="$(grep -c '^# MO-HA-END$' /etc/hosts 2>/dev/null || true)"
  if [ "${b:-0}" = "${e:-0}" ]; then
    run "sed -i '/^# MO-HA-BEGIN$/,/^# MO-HA-END$/d' /etc/hosts"
  else
    wrn "/etc/hosts 的 MO-HA 标记不成对 (BEGIN=${b:-0} END=${e:-0})：只删除标记行，保留其余内容"
    run "sed -i '/^# MO-HA-BEGIN$/d;/^# MO-HA-END$/d' /etc/hosts"
  fi
}

require_nodes(){
  [ -n "$SELF" ] && [ -n "$PEER" ] && [ -n "$SELF_IP" ] && [ -n "$PEER_IP" ] \
    || die "缺少参数: --self/--peer/--self-ip/--peer-ip"
  [ "$SELF" != "$PEER" ] || die "--self 与 --peer 不能相同（当前都是 ${SELF}）"
  [ "$SELF_IP" != "$PEER_IP" ] || die "--self-ip 与 --peer-ip 不能相同（当前都是 ${SELF_IP}）"
  case "$SELF_IP$PEER_IP" in
    *[!0-9.]*) die "--self-ip/--peer-ip 需要裸 IPv4 地址（不要写主机名或 /掩码）：${SELF_IP} ${PEER_IP}" ;;
  esac
  # 节点名直接生成 on <name> { ... }；含空格/花括号/换行会写出语法错误甚至被注入的配置。
  local _n
  for _n in "$SELF" "$PEER"; do
    case "$_n" in
      [A-Za-z0-9]*)
        case "$_n" in
          *[!A-Za-z0-9._-]*) die "--self/--peer 只允许字母数字与 . _ -（收到：${_n}）" ;;
        esac;;
      *) die "--self/--peer 必须以字母数字开头（收到：${_n}）" ;;
    esac
  done
  # 资源名会作为 /etc/drbd.d/<name>.res 的路径片段，禁止穿越与空白
  case "$DRBD_RESOURCE" in
    ""|*/*|*..*|*[[:space:]]*) die "--drbd-resource 必须是纯名称（不含 / .. 空白），收到：${DRBD_RESOURCE}" ;;
    *[!A-Za-z0-9._-]*) die "--drbd-resource 只允许字母数字与 . _ -（收到：${DRBD_RESOURCE}）" ;;
  esac
}

# 破坏性操作前的护栏：拒绝把系统盘/已挂载盘当 DRBD 裸盘（wipefs/mkfs 不可逆）。
guard_drbd_device(){
  if [ "$DRY_RUN" = 1 ]; then
    info "(dry-run) 跳过 ${DRBD_DEVICE} 占用检查（实跑时该盘或其分区已挂载会被拒绝）"
    return 0
  fi
  if ! command -v lsblk >/dev/null 2>&1; then
    die "未找到 lsblk，无法校验 ${DRBD_DEVICE} 是否已挂载，拒绝继续 wipefs/mkfs"
  fi
  local _mp
  _mp=$(lsblk -no MOUNTPOINT "$DRBD_DEVICE")
  _mp=$(printf '%s' "$_mp" | tr -d '[:space:]')
  [ -n "$_mp" ] || return 0
  die "${DRBD_DEVICE} 或其子分区已挂载（${_mp}），拒绝 wipefs/mkfs：--drbd-device 必须是专用空盘，请核对是否误填了系统盘"
}

ha_timeouts(){
  local name value
  for name in STONITH_TIMEOUT FENCE_MONITOR_TIMEOUT; do
    value="${!name}"
    [[ "$value" =~ ^[1-9][0-9]{0,3}$ ]] && [ "$value" -le 3600 ] \
      || die "$name 必须为 1..3600 的整数秒数"
  done
  # 回调应覆盖一次隔离/DC 选举；OCF 操作预算再覆盖回调，避免误超时杀死健康节点。
  DRBD_FENCE_WAIT=$((STONITH_TIMEOUT + 30))
  DRBD_DC_WAIT=$((STONITH_TIMEOUT + 60))
  DRBD_ACTION_TIMEOUT=$((STONITH_TIMEOUT + 90))
}

guard_initial_drbd_format(){
  guard_drbd_device
  if [ "$BOOTSTRAP" = 1 ] && [ "$DRY_RUN" != 1 ]; then
    if pcs resource config p_drbd >/dev/null 2>&1 || drbdadm role "$DRBD_RESOURCE" >/dev/null 2>&1; then
      die "DRBD 已启动或已由 Pacemaker 管理，拒绝 --format-drbd；已有部署请不带格式化参数续跑"
    fi
  fi
}

prepare_drbd_fencing(){
  ha_timeouts
  local version code helper_suffix="" directory
  version="$(drbdadm --version 2>/dev/null)" || version=""
  code="$(printf '%s\n' "$version" | sed -n 's/^DRBD_KERNEL_VERSION_CODE=//p')"
  case "$code" in
    0x0804??) DRBD_KERNEL_MAJOR=8;;
    0x09*) DRBD_KERNEL_MAJOR=9; helper_suffix=.9;;
    *)
      if [ "$DRY_RUN" = 1 ]; then
        info "(dry-run) 无法探测 DRBD 内核，以下暂展示 8.4 配置；正式执行按已加载内核检测"
        DRBD_KERNEL_MAJOR=8
      else
        die "无法识别 DRBD 内核版本，请先加载受支持的 8.4/9.x 模块（不按 drbdadm 工具版本推断）"
      fi;;
  esac
  [ "$DRBD9_QUORUM" != 1 ] || [ "$DRBD_KERNEL_MAJOR" = 9 ] \
    || die "DRBD9_QUORUM=1 需要 DRBD 9 内核，当前为 DRBD 8"
  directory="$DRBD_HELPER_DIR"
  if [ -z "$directory" ]; then
    directory=/usr/lib/drbd
    [ ! -x "/usr/lib64/drbd/crm-fence-peer${helper_suffix}.sh" ] \
      || directory=/usr/lib64/drbd
  fi
  DRBD_FENCE_HELPER="$directory/crm-fence-peer${helper_suffix}.sh"
  DRBD_UNFENCE_HELPER="$directory/crm-unfence-peer${helper_suffix}.sh"
  if [ "$DRY_RUN" != 1 ]; then
    [ -x "$DRBD_FENCE_HELPER" ] && [ -x "$DRBD_UNFENCE_HELPER" ] \
      || die "缺少可执行的 DRBD fencing 回调：$DRBD_FENCE_HELPER / $DRBD_UNFENCE_HELPER"
  fi
  DRBD_FENCE_NET_LINE=""; DRBD_FENCE_DISK_LINE=""
  DRBD_AFTER_RESYNC_LINE=""
  if [ "$DRBD_KERNEL_MAJOR" = 8 ]; then
    DRBD_FENCE_DISK_LINE="fencing resource-and-stonith;"
    DRBD_AFTER_RESYNC_LINE="after-resync-target \"${DRBD_UNFENCE_HELPER}\";"
  else
    DRBD_FENCE_NET_LINE="fencing resource-and-stonith;"
  fi
}

stage_drbd() (
  hdr "阶段 drbd: 配置 DRBD"
  require_nodes
  [ -b "$DRBD_DEVICE" ] || [ "$DRY_RUN" = 1 ] || die "$DRBD_DEVICE 不是块设备"
  if [ "$FORMAT_DRBD" = 1 ]; then guard_initial_drbd_format; fi
  prepare_drbd_fencing
  local quorum_opts=""
  [ "$DRBD9_QUORUM" = 1 ] && quorum_opts="
  options {
    quorum majority;
    on-no-quorum io-error;
  }"
  run "cat > /etc/drbd.d/${DRBD_RESOURCE}.res <<'DRBD'
resource ${DRBD_RESOURCE} {
  protocol C;
  device ${DRBD_VOLUME};
  disk ${DRBD_DEVICE};
  meta-disk internal;
  on ${SELF} { address ${SELF_IP}:${DRBD_PORT}; }
  on ${PEER} { address ${PEER_IP}:${DRBD_PORT}; }
  net {
    allow-two-primaries no;
    verify-alg sha256;
    ${DRBD_FENCE_NET_LINE}
    after-sb-0pri disconnect;
    after-sb-1pri disconnect;
    after-sb-2pri disconnect;
  }
  handlers {
    fence-peer \"${DRBD_FENCE_HELPER} --timeout ${DRBD_FENCE_WAIT} --dc-timeout ${DRBD_DC_WAIT}\";
    ${DRBD_AFTER_RESYNC_LINE}
    unfence-peer \"${DRBD_UNFENCE_HELPER}\";
  }
  disk {
    on-io-error detach;
    ${DRBD_FENCE_DISK_LINE}
    # 关闭动态调速；后台同步上限为 50 MiB/s，实际速度取决于磁盘和网络。
    c-plan-ahead 0;
    resync-rate 50M;
  }${quorum_opts}
}
DRBD"
  # 已有元数据时不要用 --force 覆盖：重跑本阶段会重建元数据，破坏既有卷
  if [ "$DRY_RUN" != 1 ] && drbdadm dump-md "$DRBD_RESOURCE" >/dev/null 2>&1; then
    info "检测到已有 DRBD 元数据，跳过 create-md"
  else
    if [ "$DRY_RUN" = 1 ]; then
      runq "drbdadm create-md ${DRBD_RESOURCE} --force"
    else
      # 管道会把退出码换成 tail 的 0，必须显式取命令自身状态，否则 create-md 失败也报「配置完成」。
      local _mdout="" _mdrc=0
      _mdout=$(drbdadm create-md "${DRBD_RESOURCE}" --force 2>&1) || _mdrc=$?
      printf '%s\n' "$_mdout" | tail -3
      [ "$_mdrc" = 0 ] || die "drbdadm create-md ${DRBD_RESOURCE} 失败（设备被占用？元数据区不足？），请查看上方输出"
    fi
  fi

  if [ "$DRY_RUN" != 1 ] && [ "$FORMAT_DRBD" = 1 ]; then
    if [ "$BOOTSTRAP" != 1 ]; then
      wrn "--format-drbd 只应在 bootstrap 节点执行；本机跳过格式化"
    else
      info "首次初始化 DRBD 数据卷 (up -> promote --force -> mkfs.xfs -> down)"
      # 首次格式化早于 CIB/STONITH 建立：只对这次离线初始化使用临时配置。
      # 正式文件始终保留隔离保护；临时资源必须在离开本阶段前停止。
      local init_dir init_conf init_up=0
      init_dir="$(mktemp -d "${TMPDIR:-/tmp}/mo-ha-init.XXXXXX")"
      init_conf="$init_dir/init.conf"
      trap 'if [ "$init_up" = 1 ]; then drbdadm -c "$init_conf" down "$DRBD_RESOURCE" >/dev/null 2>&1 || true; fi; rm -rf -- "$init_dir"' EXIT
      # -c 使用独立配置，不会继承 global_common.conf；否则每次 up/primary/down
      # 都可能退回 usage-count ask，使首次初始化停在安装统计的交互提示。
      printf 'global { usage-count no; }\n' > "$init_conf"
      sed -E '/^[[:space:]]*(fence-peer|unfence-peer|after-resync-target)[[:space:]]/d; s/fencing resource-and-stonith;/fencing dont-care;/; s/quorum majority;/quorum off;/' \
        "/etc/drbd.d/${DRBD_RESOURCE}.res" >> "$init_conf"
      init_up=1
      runq "drbdadm -c '$init_conf' up ${DRBD_RESOURCE}" || die "drbdadm up ${DRBD_RESOURCE} 失败，请查看 dmesg / drbdadm status"
      if blkid "$DRBD_VOLUME" >/dev/null 2>&1; then
        info "$DRBD_VOLUME 已有文件系统，跳过 mkfs"
      else
        runq "drbdadm -c '$init_conf' primary --force ${DRBD_RESOURCE}" || die "drbdadm primary ${DRBD_RESOURCE} --force 失败，无法格式化"
        runq "mkfs.xfs -f ${DRBD_VOLUME}" || die "mkfs.xfs ${DRBD_VOLUME} 失败（设备被占用？缺 xfsprogs？）"
        # mkfs 返回 0 仍要确认文件系统真的在：否则会把裸设备交给 p_fs，挂载失败反复重启，
        # 在 stonith-enabled=true 的集群上可能升级为 fence 健康节点。
        local _fsok=0
        for _i in 1 2 3; do
          sync
          if blkid "$DRBD_VOLUME" >/dev/null 2>&1; then _fsok=1; break; fi
          sleep 1
        done
        [ "$_fsok" = 1 ] || die "mkfs 后仍未在 $DRBD_VOLUME 检测到文件系统，拒绝交给集群；请检查设备与 xfsprogs"
      fi
      runq "drbdadm -c '$init_conf' down ${DRBD_RESOURCE}" || die "drbdadm down ${DRBD_RESOURCE} 失败，拒绝继续交给 Pacemaker 接管"
      init_up=0
      ok "DRBD 数据卷就绪，交由 Pacemaker 管理"
    fi
  elif [ "$DRY_RUN" != 1 ] && [ "$BOOTSTRAP" = 1 ]; then
    wrn "未指定 --format-drbd：若 $DRBD_VOLUME 尚未格式化，集群启动 p_fs 会失败"
  fi
  info "DRBD 资源 ${DRBD_RESOURCE}(${DRBD_VOLUME} <- ${DRBD_DEVICE}) 配置完成"
)

# ----------------------------- 阶段 cluster --------------------------------
stage_cluster(){
  hdr "阶段 cluster: corosync/pacemaker"
  require_nodes
  ha_timeouts
  # 集群组建失败必须致命：否则后续阶段在没有 corosync/pacemaker 的机器上继续跑，
  # 报错点会指向 fencing/资源，而不是真正的根因。
  cluster_cmd(){
    if ! runq "$1"; then
      # $1 含 hacluster 密码（pcs host auth -p '...'），必须先脱敏再打印
      die "集群组建失败：$(mask_secrets "$1")
       常见原因：两台 --hacluster-password 不一致、主机名/IP 解析不通、pcsd 未运行。
       请修正后重跑：--stages cluster"
    fi
  }
  [ -n "$HACLUSTER_PW" ] || die "缺少 --hacluster-password（pcsd 认证密码，两台必须一致）"
  runq "systemctl enable --now pcsd"
  runq "echo 'hacluster:${HACLUSTER_PW}' | chpasswd"
  runq "mkdir -p /etc/corosync /var/lib/corosync /var/log/corosync"
  runq "systemctl enable corosync pacemaker"
  if [ "$BOOTSTRAP" != 1 ]; then
    info "非引导节点：pcsd/hacluster 已就绪，等待引导节点组建集群"
    return 0
  fi
  cluster_cmd "pcs host auth $SELF $PEER -u hacluster -p '${HACLUSTER_PW}'"
  # pcs cluster setup 只会分发已存在的 corosync authkey，缺失时需先生成
  if [ "$DRY_RUN" != 1 ]; then
    runq "mkdir -p /etc/corosync /var/lib/corosync /var/log/corosync"
    [ -f /etc/corosync/authkey ] || runq "corosync-keygen -l"
  fi
  # 保留节点名作为集群身份，但通信地址使用裸 IP；断网重启时不能依赖 DNS/hosts 解析。
  cluster_cmd "pcs cluster setup $CLUSTER $SELF addr=$SELF_IP $PEER addr=$PEER_IP --start --enable --force"
  cluster_cmd "pcs cluster enable --all"
  info "等待集群形成 quorum..."
  if [ "$DRY_RUN" != 1 ]; then
    _q=0
    for _i in $(seq 1 40); do
      if corosync-quorumtool -s 2>/dev/null | grep -qE "Quorate:[[:space:]]+Yes"; then _q=1; break; fi
      sleep 2
    done
    [ "$_q" = 1 ] && ok "quorum 已形成" || wrn "等待 quorum 超时，请检查 corosync 与 /etc/hosts"
    # 只有真正 quorate 时才继续，否则后面的资源会在无 quorum 的集群上创建
    [ "$_q" = 1 ] || die "未取得 quorum，请修正 corosync/网络后重跑：--stages cluster"
  fi
  runq "pcs property set stonith-enabled=true"
  runq "pcs property set stonith-timeout=${STONITH_TIMEOUT}s"
  runq "pcs property set no-quorum-policy=stop"
  [ "$FENCE_SCENARIO" = "sbd" ] && runq "pcs property set have-watchdog=true"
  info "集群 $CLUSTER 组建完成"
}

# ----------------------------- 阶段 fence ----------------------------------
fence_agent_for(){
  case "$FENCE_SCENARIO" in
    ipmi) echo fence_ipmilan;; redfish) echo fence_redfish;;
    pdu) echo fence_apc_snmp;; vmware) echo fence_vmware_rest;;
    proxmox) echo fence_pve;; virsh) echo fence_virsh;;
    aws) echo fence_aws;; azure) echo fence_azure_arm;;
    gce) echo fence_gce;; aliyun) echo fence_aliyun;;
    custom_http) echo "${FENCE_AGENT_OVERRIDE:-fence_lima}";;
    sbd) echo external/sbd;;
    *) die "未知 fencing 场景: $FENCE_SCENARIO";;
  esac
}
extra_params(){ [ -n "$FENCE_EXTRA" ] && echo "$FENCE_EXTRA" | tr ',' ' ' || echo ""; }

prepare_proxmox_fence(){
  [ "$FENCE_SCENARIO" = proxmox ] || return 0
  # 兼容只填写管理入口的 https URL；端口仍由 FENCE_EXTRA 的 ipport 指定。
  FENCE_SHARED_IP="${FENCE_SHARED_IP#https://}"
  FENCE_SHARED_IP="${FENCE_SHARED_IP%/}"
  case "$FENCE_SHARED_IP" in
    ""|*/*|*:*|*[[:space:]]*) die "Proxmox 管理入口请填域名/IP，端口用 FENCE_EXTRA=ipport=... 指定";;
  esac
  case ",$FENCE_EXTRA," in
    *,vmtype=*) ;;
    *) FENCE_EXTRA="${FENCE_EXTRA:+$FENCE_EXTRA,}vmtype=qemu";;
  esac
  if [ "$DRY_RUN" = 1 ]; then
    info "(dry-run) 检查 fence_pve：新版启用原生宿主机发现，已知旧版备份后修补 nodename 冲突"
    return 0
  fi
  local pve_agent auto_param
  pve_agent="$(command -v fence_pve)" || die "未安装 fence_pve，请先执行 pkg 阶段"
  command -v python3 >/dev/null 2>&1 || die "Proxmox 代理兼容检查需要 python3"
  auto_param="$(python3 - "$pve_agent" <<'MO_HA_PVE_PY'
#!/usr/bin/env python3
"""Enable native PVE host discovery, or repair the known legacy nodename bug."""
import hashlib
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET


def metadata(agent):
    result = subprocess.run(
        [str(agent), "--action=metadata"], stdin=subprocess.DEVNULL,
        capture_output=True, text=True, check=True, timeout=20,
    )
    root = ET.fromstring(result.stdout)
    if root.tag != "resource-agent":
        raise ValueError("代理未返回 resource-agent 元数据")
    return {p.get("name") for p in root.findall("./parameters/parameter")}


def replace(agent, content):
    """Preserve permissions, ownership and attributes; publish with one rename."""
    attributes = agent.stat()
    descriptor, temporary = tempfile.mkstemp(prefix=".fence_pve.mo-ha-", dir=agent.parent)
    os.close(descriptor)
    try:
        shutil.copy2(agent, temporary)
        Path(temporary).write_bytes(content)
        os.chown(temporary, attributes.st_uid, attributes.st_gid)
        os.replace(temporary, agent)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def prepare(agent):
    agent = agent.resolve(strict=True)
    parameters = metadata(agent)
    if "pve_node_auto" in parameters:
        # Native discovery re-resolves the host for every fencing invocation.
        return "pve_node_auto=1"
    original = agent.read_bytes()
    source = original.decode("utf-8")
    already_patched = re.search(
        r'(?m)^[ \t]*# MO-HA compatibility:[^\n]*\n(?:[ \t]*#[^\n]*\n)*'
        r'[ \t]*options\["--nodename"\] = None[ \t]*$', source,
    )
    # Only the known old implementation is patched. Unknown layouts fail before
    # touching the file, rather than guessing how a fencing agent operates.
    pattern = re.compile(
        r'(?m)^(?P<indent>[ \t]+)if "--nodename" not in options or not '
        r'options\["--nodename"\]:\n(?P=indent)[ \t]+options\["--nodename"\] = None[ \t]*$'
    )
    patched, count = pattern.subn(
        lambda match: match["indent"] + "# MO-HA compatibility: discover PVE host by VMID, not Pacemaker nodename.\n"
        + match["indent"] + 'options["--nodename"] = None', source,
    )
    if (not already_patched and count != 1) or 'if options["--nodename"] is None:' not in source:
        raise ValueError("无法识别旧版 fence_pve 自动发现逻辑，请使用支持 pve_node_auto 的版本")
    # Give both nodes the same input contract even when their agent versions
    # differ: old agents must accept the native auto parameter used by new ones.
    input_pattern = re.compile(
        r'(?m)^(?P<indent>[ \t]+)options = check_input\(device_opt, process_input\(device_opt\)\)$'
    )
    patched, input_count = input_pattern.subn(
        lambda match: match["indent"] + 'all_opt["pve_node_auto"] = dict(getopt="A", longopt="pve-node-auto", '
        'required="0", type="boolean", help="-A, --pve-node-auto  Automatically select Proxmox node", '
        'shortdesc="Automatically select Proxmox node", order=2)\n'
        + match["indent"] + 'device_opt.append("pve_node_auto")\n' + match[0], patched,
    )
    if input_count != 1:
        raise ValueError("无法识别旧版 fence_pve 参数入口，停止自动修补")
    compile(patched, str(agent), "exec")
    backup = agent.with_name(agent.name + ".mo-ha-" + hashlib.sha256(original).hexdigest()[:12] + ".bak")
    if backup.exists():
        if backup.read_bytes() != original:
            raise ValueError("同名代理备份内容不匹配，停止修改")
    else:
        shutil.copy2(agent, backup)
    replace(agent, patched.encode("utf-8"))
    try:
        if metadata(agent) != parameters | {"pve_node_auto"}:
            raise ValueError("修补后的代理元数据不符合预期")
    except Exception:
        replace(agent, original)
        raise
    print("旧版 fence_pve 已启用 VMID 自动发现；原件备份：" + str(backup), file=sys.stderr)
    return "pve_node_auto=1"


if __name__ == "__main__":
    try:
        if len(sys.argv) != 2:
            raise ValueError("用法：prepare-proxmox-agent.py /path/to/fence_pve")
        print(prepare(Path(sys.argv[1])))
    except (OSError, ValueError, SyntaxError, ET.ParseError, subprocess.SubprocessError) as error:
        print("ERROR: fence_pve 自动发现准备失败：" + str(error), file=sys.stderr)
        sys.exit(1)
MO_HA_PVE_PY
)"
  if [ -n "$auto_param" ]; then
    case ",$FENCE_EXTRA," in
      *,pve_node=*|*,pve_node_auto=*) ;; # 尊重用户已明确指定的新版参数
      *) FENCE_EXTRA="${FENCE_EXTRA:+$FENCE_EXTRA,}$auto_param";;
    esac
  fi
  info "fence_pve 已准备宿主机定位参数"
}

publish_cib_changes(){
  local original="$1" updated="$2" validation="$3"
  # 仅从校验副本剔除瞬时运行态；提交副本与原快照保持完整。
  runq "python3 - '$updated' '$validation' <<'MO_HA_CIB_PY'
import sys
import xml.etree.ElementTree as ET
tree = ET.parse(sys.argv[1])
root = tree.getroot()
for status in root.findall('status'):
    root.remove(status)
tree.write(sys.argv[2], encoding='utf-8', xml_declaration=True)
MO_HA_CIB_PY"
  runq "crm_verify -x '$validation' -VVVV"
  runq "pcs cluster cib-push '$updated' diff-against='$original'"
}

stage_fence() (
  hdr "阶段 fence: STONITH ($FENCE_SCENARIO)"
  [ -n "$FENCE_SCENARIO" ] || die "缺少 --fence-scenario"
  [ "$BOOTSTRAP" = 1 ] || { info "非引导节点：跳过 fencing 创建"; return 0; }
  require_nodes
  ha_timeouts
  case "$FENCE_SCENARIO" in
    ipmi|redfish|custom_http)
      [ -n "$FENCE_TARGET_SELF" ] && [ -n "$FENCE_TARGET_PEER" ] || die "$FENCE_SCENARIO 需要 --fence-target-self/peer";;
    sbd) [ -n "$SBD_DEVICE" ] || die "sbd 场景需要 --sbd-device";;
    *)
      [ -n "$FENCE_PLUG_SELF" ] && [ -n "$FENCE_PLUG_PEER" ] || die "$FENCE_SCENARIO 需要 --fence-plug-self/peer"
      case "$FENCE_SCENARIO" in
        pdu|vmware|proxmox|virsh) [ -n "$FENCE_SHARED_IP" ] || die "$FENCE_SCENARIO 需要 --fence-shared-ip";;
      esac;;
  esac
  prepare_proxmox_fence
  local agent; agent="$(fence_agent_for)"; local ex; ex="$(extra_params)"
  # 先验证代理元数据，避免代理缺失时先关闭 STONITH 或删除现有设备。
  if [ "$FENCE_SCENARIO" != sbd ]; then runq "pcs stonith describe '$agent' >/dev/null"; fi
  # 在离线 CIB 中替换设备；失败保留线上原设备和原隔离策略，不关闭 STONITH。
  local cib_dir cib_original cib_work
  if [ "$DRY_RUN" = 1 ]; then
    cib_dir="${TMPDIR:-/tmp}/mo-ha-fence.dry-run"
  else
    umask 077
    cib_dir="$(mktemp -d "${TMPDIR:-/tmp}/mo-ha-fence.XXXXXX")"
    trap 'rm -rf -- "$cib_dir"' EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM
  fi
  cib_original="$cib_dir/original.xml"; cib_work="$cib_dir/updated.xml"
  runq "pcs cluster cib '$cib_original'"
  runq "cp '$cib_original' '$cib_work'"
  stonith_create(){
    local command="${1#pcs }"
    if ! runq "pcs -f '$cib_work' $command timeout=${FENCE_MONITOR_TIMEOUT}s"; then
      die "fencing 设备创建失败，未提交配置，线上原隔离配置保持不变。
       请修正参数后重跑：--stages fence"
    fi
  }
  local fence_id
  for fence_id in "fence_$SELF" "fence_$PEER" "fence_${DRBD_RESOURCE}"; do
    if [ "$DRY_RUN" = 1 ] || pcs -f "$cib_work" stonith config "$fence_id" >/dev/null 2>&1; then
      runq "pcs -f '$cib_work' stonith delete '$fence_id'"
    fi
  done
  local addrkey="${FENCE_ADDR_PARAM}"
  if [ -z "$addrkey" ]; then
    case "$FENCE_SCENARIO" in custom_http) addrkey=ip;; *) addrkey=ipaddr;; esac
  fi

  if [ "$FENCE_SCENARIO" = "sbd" ]; then
    [ -n "$SBD_DEVICE" ] || die "sbd 场景需要 --sbd-device"
    # 配置路径按发行版：RHEL 系 /etc/sysconfig/sbd，Debian/Ubuntu 为 /etc/default/sbd
    local sbd_conf=/etc/sysconfig/sbd
    [ "$OS_FAMILY" = deb ] && sbd_conf=/etc/default/sbd
    if [ "$DRY_RUN" != 1 ] && [ ! -e "$WATCHDOG" ]; then
      die "sbd 需要 watchdog 设备 ${WATCHDOG}（modprobe softdog，或改用硬件 watchdog）"
    fi
    run "cat > $sbd_conf <<SBD
SBD_DEVICE=\"${SBD_DEVICE}\"
SBD_PACEMAKER=yes
SBD_STARTMODE=always
SBD_WATCHDOG_DEV=${WATCHDOG}
SBD_WATCHDOG_TIMEOUT=5
SBD"
    runq "systemctl enable --now sbd" || die "sbd 启动失败：请确认 sbd 已安装且 $sbd_conf 与 watchdog 正确"
    stonith_create "pcs stonith create fence_${DRBD_RESOURCE} external/sbd pcmk_delay_max=${FENCE_DELAY_MAX} op monitor interval=30s"
    runq "pcs -f '$cib_work' property set stonith-enabled=true stonith-timeout=${STONITH_TIMEOUT}s"
    publish_cib_changes "$cib_original" "$cib_work" "$cib_dir/validate.xml"
    return 0
  fi

  case "$FENCE_SCENARIO" in
    ipmi)
      [ -n "$FENCE_TARGET_SELF" ] && [ -n "$FENCE_TARGET_PEER" ] || die "ipmi 需要 --fence-target-self/peer"
      stonith_create "pcs stonith create fence_$SELF fence_ipmilan $addrkey=$FENCE_TARGET_SELF login=$FENCE_LOGIN passwd='$FENCE_PASSWORD' $ex pcmk_host_list=$SELF pcmk_delay_max=$FENCE_DELAY_MAX op monitor interval=30s"
      stonith_create "pcs stonith create fence_$PEER fence_ipmilan $addrkey=$FENCE_TARGET_PEER login=$FENCE_LOGIN passwd='$FENCE_PASSWORD' $ex pcmk_host_list=$PEER pcmk_delay_max=$FENCE_DELAY_MAX op monitor interval=30s";;
    redfish)
      [ -n "$FENCE_TARGET_SELF" ] && [ -n "$FENCE_TARGET_PEER" ] || die "redfish 需要 --fence-target-self/peer"
      stonith_create "pcs stonith create fence_$SELF fence_redfish $addrkey=$FENCE_TARGET_SELF username=$FENCE_LOGIN password='$FENCE_PASSWORD' $ex pcmk_host_list=$SELF pcmk_delay_max=$FENCE_DELAY_MAX op monitor interval=30s"
      stonith_create "pcs stonith create fence_$PEER fence_redfish $addrkey=$FENCE_TARGET_PEER username=$FENCE_LOGIN password='$FENCE_PASSWORD' $ex pcmk_host_list=$PEER pcmk_delay_max=$FENCE_DELAY_MAX op monitor interval=30s";;
    custom_http)
      [ -n "$FENCE_TARGET_SELF" ] && [ -n "$FENCE_TARGET_PEER" ] || die "custom_http 需要 --fence-target-self/peer"
      stonith_create "pcs stonith create fence_$SELF $agent $addrkey=$FENCE_TARGET_SELF $ex pcmk_host_list=$SELF pcmk_delay_max=$FENCE_DELAY_MAX op monitor interval=30s"
      stonith_create "pcs stonith create fence_$PEER $agent $addrkey=$FENCE_TARGET_PEER $ex pcmk_host_list=$PEER pcmk_delay_max=$FENCE_DELAY_MAX op monitor interval=30s";;
    *)
      [ -n "$FENCE_PLUG_SELF" ] && [ -n "$FENCE_PLUG_PEER" ] || die "$FENCE_SCENARIO 需要 --fence-plug-self/peer"
      local map="$SELF:$FENCE_PLUG_SELF;$PEER:$FENCE_PLUG_PEER" ipar=""
      case "$FENCE_SCENARIO" in
        pdu) [ -n "$FENCE_SHARED_IP" ] || die "pdu 需要 --fence-shared-ip"; ipar="ipaddr=$FENCE_SHARED_IP login=$FENCE_LOGIN passwd='$FENCE_PASSWORD' snmp_community=$FENCE_SNMP";;
        vmware|proxmox) [ -n "$FENCE_SHARED_IP" ] || die "$FENCE_SCENARIO 需要 --fence-shared-ip"; ipar="ipaddr=$FENCE_SHARED_IP login=$FENCE_LOGIN passwd='$FENCE_PASSWORD'";;
        virsh) [ -n "$FENCE_SHARED_IP" ] || die "virsh 需要 --fence-shared-ip"; ipar="ipaddr=$FENCE_SHARED_IP login=$FENCE_LOGIN";;
      esac
      stonith_create "pcs stonith create fence_$DRBD_RESOURCE $agent $ipar $ex pcmk_host_map='$map' pcmk_delay_max=$FENCE_DELAY_MAX op monitor interval=30s";;
  esac
  runq "pcs -f '$cib_work' property set stonith-enabled=true stonith-timeout=${STONITH_TIMEOUT}s"
  publish_cib_changes "$cib_original" "$cib_work" "$cib_dir/validate.xml"
)

# ----------------------------- 阶段 resources ------------------------------
stage_resources() (
  hdr "阶段 resources: DRBD/FS/MatrixOne 资源"
  [ "$BOOTSTRAP" = 1 ] || { info "非引导节点：跳过资源创建"; return 0; }
  ha_timeouts
  # Shell 顺序不等于调度事务：所有删除、创建、克隆、约束均只修改离线副本。
  # 子 shell 的 EXIT trap 覆盖成功、错误和中断；CIB 含 fencing 凭据，目录/文件限 root 访问。
  local cib_dir cib_original cib_work cib_validate
  if [ "$DRY_RUN" = 1 ]; then
    cib_dir="${TMPDIR:-/tmp}/mo-ha-cib.dry-run"
  else
    umask 077
    cib_dir="$(mktemp -d "${TMPDIR:-/tmp}/mo-ha-cib.XXXXXX")"
    trap 'rm -rf -- "$cib_dir"' EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM
  fi
  cib_original="$cib_dir/original.xml"; cib_work="$cib_dir/updated.xml"
  cib_validate="$cib_dir/validate.xml"
  runq "pcs cluster cib '$cib_original'"
  runq "cp '$cib_original' '$cib_work'"
  cib_cmd(){ runq "pcs -f '$cib_work' $*"; }
  # 仅删除副本中实际存在的资源；真正的删除失败仍必须退出，不能 || true。
  local resource_id
  for resource_id in p_mo p_vip p_fs p_drbd-clone p_drbd; do
    if [ "$DRY_RUN" = 1 ] || pcs -f "$cib_work" resource config "$resource_id" >/dev/null 2>&1; then
      cib_cmd "resource delete $resource_id"
    fi
  done
  cib_cmd "resource create p_drbd ocf:linbit:drbd drbd_resource=$DRBD_RESOURCE unfence_if_all_uptodate=true op start interval=0 timeout=${DRBD_ACTION_TIMEOUT}s op stop interval=0 timeout=${DRBD_ACTION_TIMEOUT}s op promote interval=0 timeout=${DRBD_ACTION_TIMEOUT}s op demote interval=0 timeout=${DRBD_ACTION_TIMEOUT}s op monitor interval=20s role=Promoted timeout=30s op monitor interval=30s role=Unpromoted timeout=30s"
  # linbit drbd RA 要求显式 clone-max<=2、notify；严格限制全局只有一个 Primary。
  cib_cmd "resource promotable p_drbd p_drbd-clone clone-max=2 clone-node-max=1 promoted-max=1 promoted-node-max=1 notify=true"
  cib_cmd "resource create p_fs Filesystem device=$DRBD_VOLUME directory=$DATA_DIR fstype=xfs op monitor interval=20s"
  cib_cmd "constraint colocation add p_fs with p_drbd-clone INFINITY with-rsc-role=Promoted"
  cib_cmd "constraint order promote p_drbd-clone then start p_fs"
  if [ "$NO_MO" != 1 ]; then
    runq "mkdir -p $MO_DIR"
    cib_cmd "resource create p_mo ocf:heartbeat:docker-compose binpath=$(compose_wrapper_path) dirpath=$MO_DIR ymlfile=docker-compose.yaml op monitor interval=30s timeout=60s op start timeout=$MO_START_TIMEOUT interval=0 op stop timeout=120s"
    cib_cmd "constraint colocation add p_mo with p_fs INFINITY"
    cib_cmd "constraint order start p_fs then start p_mo"
  fi
  if [ -n "$VIP" ]; then
    # ${VIP##*/} 在无 "/" 时返回原串，会把整个 IP 当成 netmask 传给 IPaddr2
    case "$VIP" in
      */*) ;;
      *)   die "--vip 需要 CIDR 形式(如 10.0.0.100/24)，当前值：$VIP" ;;
    esac
    local _vip_addr="${VIP%%/*}" _vip_mask="${VIP##*/}"
    [ -n "$_vip_addr" ] && [ -n "$_vip_mask" ] || die "--vip 缺少地址或掩码部分：$VIP"
    cib_cmd "resource create p_vip IPaddr2 ip=$_vip_addr cidr_netmask=$_vip_mask op monitor interval=20s"
    cib_cmd "constraint colocation add p_vip with p_fs INFINITY"
    cib_cmd "constraint order start p_fs then start p_vip"
    # MO 端口若绑定在 VIP 上，必须等 VIP 就绪后再起容器，否则 docker 会报
    # "cannot assign requested address" 启动失败
    [ "$NO_MO" = 1 ] || cib_cmd "constraint order start p_vip then start p_mo"
  fi
  # crm_verify 遇到 <status> 会额外模拟调度；刚建集群的运行态快照可能尚未就绪，
  # 进而因调度警告返回 78。部署前只校验配置，不把这份瞬时状态当成配置错误。
  # 仅处理校验副本；差异提交仍使用原来的完整副本，不能向线上提交 status 删除。
  # 提交差异而非 --config 全量覆盖，保留其他操作者的无关并发修改。
  # 冲突或不支持 diff-against 时直接退出；不得回退为强制覆盖或清理在线失败记录。
  publish_cib_changes "$cib_original" "$cib_work" "$cib_validate"
  info "完整资源配置已一次提交；由 check 阶段验证异步启动结果"
)

# ----------------------------- 阶段 teardown -------------------------------
stage_teardown(){
  hdr "阶段 teardown: 清理既有 HA 配置 (危险)"
  info "将销毁集群、DRBD、MO 容器与数据目录挂载（保留 $DRBD_DEVICE 数据需自行 backup）"
  if [ "$DRY_RUN" != 1 ] && [ "$ASSUME_YES" != 1 ]; then
    if [ ! -t 0 ]; then die "teardown 是破坏性操作(含 wipefs $DRBD_DEVICE)，非交互执行必须显式加 --yes"; fi
    printf "  确认销毁？输入 yes 继续: "
    local _a=""; read -r _a || true
    [ "$_a" = "yes" ] || die "已取消 teardown"
  fi
  runq "pcs cluster stop --all"
  runq "pcs cluster destroy --all"
  runq "systemctl disable --now corosync pacemaker pcsd sbd"
  runq "docker rm -f matrixone"
  runq "drbdadm down $DRBD_RESOURCE"
  # 先卸载再抹盘：否则会抹掉一个仍挂载着的文件系统的超级块，而 umount 又会失败。
  if [ "$DRY_RUN" = 1 ] || mountpoint -q "$DATA_DIR" 2>/dev/null; then
    run "umount $DATA_DIR" || die "umount $DATA_DIR 失败（仍被占用）：已跳过 wipefs，请手工卸载后重跑 teardown"
  fi
  guard_drbd_device
  runq "drbdadm wipe-md $DRBD_RESOURCE"
  runq "wipefs -a $DRBD_DEVICE"
  runq "rm -f /etc/drbd.d/$DRBD_RESOURCE.res /etc/modules-load.d/drbd.conf"
  # stage_pre / stage_fence 写入的其它残留一并清理
  strip_hosts_block
  runq "rm -f /etc/sysctl.d/90-mo-ha.conf /etc/sysconfig/sbd /etc/default/sbd"
  runq "sysctl --system"
  runq "rm -rf $MO_DIR"
  info "teardown 完成"
}

# ----------------------------- 阶段 check ----------------------------------
get_crm_prop(){ crm_attribute --type crm_config --query --name "$1" 2>/dev/null | sed -n 's/.*value=\(.*\)/\1/p' | tail -1; }
# wait_for <secs> <shell-cmd>：轮询命令直到成功或超时
wait_for(){
  local t="$1"; shift; local i=0
  while [ "$i" -lt "$t" ]; do
    if eval "$*" >/dev/null 2>&1; then return 0; fi
    sleep 5; i=$((i+5))
  done
  eval "$*" >/dev/null 2>&1
}
stage_check(){
  hdr "阶段 check: 部署后检查"
  CHECK_RC=0
  # dry-run 只做预览：不执行 pcs/corosync 查询，也不等待资源稳态
  if [ "$DRY_RUN" = 1 ]; then
    info "(dry-run) 检查阶段：不执行集群查询、不等待资源稳态"
    info "  正式执行时检查：quorum / stonith-enabled / no-quorum-policy / fencing 节点覆盖 / DRBD Promoted / p_fs / p_mo"
    return 0
  fi
  type -P pcs >/dev/null 2>&1 || die "无 pcs，无法执行 check 阶段"

  # 资源刚创建时需要时间进入稳态（DRBD promote / FS 挂载 / MO 拉起容器）
  local settle="${CHECK_SETTLE_SEC:-180}"
  if [ "$DRY_RUN" != 1 ] && [ "$BOOTSTRAP" = 1 ] && [ "$settle" -gt 0 ]; then
    info "等待集群资源进入稳态（最多 ${settle}s）..."
    local _s=0
    while [ "$_s" -lt "$settle" ]; do
      pcs status 2>/dev/null | grep -qE "Starting|Stopping|Pending" || break
      sleep 5; _s=$((_s+5))
    done
    if [ "$_s" -lt "$settle" ]; then info "资源已稳定（约 ${_s}s）"; else wrn "等待资源稳定超时（${settle}s）"; fi
  fi

  if corosync-quorumtool -s 2>/dev/null | grep -qE "Quorate:[[:space:]]+Yes"; then ok "quorum 就绪"; else bad "无 quorum"; fi
  local en; en="$(get_crm_prop stonith-enabled)"; case "$en" in ""|"(null)"|"unset") en=true;; esac
  [ "$en" = "true" ] && ok "stonith-enabled=true" || bad "stonith-enabled=$en"
  local nq; nq="$(get_crm_prop no-quorum-policy)"; case "$nq" in ""|"(null)"|"unset") nq=stop;; esac
  [ "$nq" = "stop" ] && ok "no-quorum-policy=stop" || wrn "no-quorum-policy=$nq"

  wait_for 120 "pcs stonith status 2>&1 | grep -q 'Started'" \
    && ok "fencing 设备已 Started" || bad "fencing 设备未 Started"
  # 节点覆盖：只解析 pcmk_host_list / pcmk_host_map 的值。
  # 不能用全文 grep 节点名——资源名本身叫 fence_<node>，会让检查恒为真。
  local cov
  cov="$(pcs stonith config 2>/dev/null | awk '
    {
      while (match($0, /(pcmk_host_list|pcmk_host_map)=[^[:space:]]+/)) {
        s = substr($0, RSTART, RLENGTH); sub(/^[^=]*=/, "", s)
        n = split(s, a, /[;:,]/)
        for (i = 1; i <= n; i++) { gsub(/"/, "", a[i]); if (a[i] != "") print a[i] }
        $0 = substr($0, RSTART + RLENGTH)
      }
    }' | sort -u)"
  for n in "$SELF" "$PEER"; do
    if printf '%s\n' "$cov" | grep -qxF "$n"; then ok "节点 $n 已被 fencing 覆盖"; else bad "节点 $n 未被 fencing 覆盖"; fi
  done
  pcs status 2>&1 | grep -qi "Failed Actions" && bad "存在 Failed Actions" || ok "无 Failed Actions"

  wait_for 180 "pcs status 2>&1 | grep -qE 'Promoted: \[ *[^] ]'" \
    && ok "DRBD 已提升为 Primary(Promoted)" || wrn "DRBD 未 Promoted（初始化/同步中属正常，稍后复查）"
  wait_for 180 "pcs status 2>&1 | grep -qE 'p_fs.*Started'" \
    && ok "p_fs Started" || wrn "p_fs 未 Started"
  if [ "$NO_MO" != 1 ]; then
    local mow="${CHECK_MO_WAIT:-300}"
    wait_for "$mow" "pcs status 2>&1 | grep -qE 'p_mo.*Started'" \
      && ok "p_mo Started" || wrn "p_mo 未 Started（首次拉镜像较慢时属正常；确认 $MO_DIR/docker-compose.yaml 已下发）"
    # 数据面：docker-compose RA 的 monitor 只看 State.Running，容器 Started
    # 不等于 SQL 已可连接（MO 可能仍在 bootstrap/迁移），单独探一次端口与登录。
    local maddr=127.0.0.1
    [ -n "$MO_BIND_IP" ] && [ "$MO_BIND_IP" != "0.0.0.0" ] && maddr="$MO_BIND_IP"
    if wait_for 120 "(exec 3<>/dev/tcp/$maddr/$MO_PORT)" || wait_for 60 "(exec 3<>/dev/tcp/127.0.0.1/$MO_PORT)"; then
      ok "MO 客户端端口 $maddr:$MO_PORT 已监听"
      local mcli="" mdocker=0
      command -v mysql >/dev/null 2>&1 && mcli=mysql
      if [ -z "$mcli" ] && docker exec matrixone sh -c 'command -v mysql >/dev/null 2>&1' 2>/dev/null; then
        mcli=mysql; mdocker=1
      fi
      # 口令都不进 argv(-p<pw> / docker exec -e MYSQL_PWD=... 在 Linux 上 ps 可见)：
      # 宿主用 MYSQL_PWD 环境变量；容器内经 stdin 下发临时 cnf。
      mo_sql_login(){
        if [ "$mdocker" = 1 ]; then
          printf '[client]\npassword=%s\n' "$MO_ROOT_PASSWORD" | \
            docker exec -i matrixone sh -c '
              umask 077
              cat > /tmp/.mo-ha.cnf
              mysql --defaults-extra-file=/tmp/.mo-ha.cnf "$@"; rc=$?
              rm -f /tmp/.mo-ha.cnf
              exit $rc' sh "$@"
        else
          MYSQL_PWD="$MO_ROOT_PASSWORD" mysql "$@"
        fi
      }
      if [ -z "$MO_ROOT_PASSWORD" ]; then
        info "未提供 --mo-root-password，跳过 SQL 登录检查（可手工：MYSQL_PWD=... mysql -h127.0.0.1 -P$MO_PORT -uroot）"
      elif [ -z "$mcli" ]; then
        info "未找到 mysql 客户端，跳过 SQL 登录检查（宿主或容器内均可）"
      elif mo_sql_login -h127.0.0.1 -P"$MO_PORT" -uroot -N -e 'select 1' 2>/dev/null | grep -qx 1; then
        ok "MO SQL 可登录（-uroot）"
      else
        wrn "MO SQL 登录失败：确认 --mo-root-password 与端口 $MO_PORT"
      fi
    else
      wrn "MO 客户端端口 $maddr:$MO_PORT 未监听（MO 可能仍在启动）"
    fi
  fi
  # --vip 是对外入口：p_vip Started 不代表地址真的在本机网卡上，
  # 若同时把 --mo-bind-ip 指到 VIP，外部会在"检查全绿"的情况下连不上。
  if [ -n "$VIP" ]; then
    local _va="${VIP%%/*}"
    if wait_for 60 "pcs status 2>&1 | grep -qE 'p_vip.*Started'"; then
      ok "VIP 资源 p_vip Started (${_va})"
      if command -v ip >/dev/null 2>&1; then
        if ip -o addr show 2>/dev/null | grep -q "[[:space:]]${_va}/"; then
          ok "VIP ${_va} 已配置在本机网卡上"
        else
          wrn "VIP ${_va} 未出现在本机网卡上：请检查 IPaddr2 参数(掩码/网卡)与 p_vip 日志"
        fi
      else
        info "未找到 ip 命令，跳过 VIP 地址确认（可手工 ip addr show 核对 ${_va}）"
      fi
    else
      wrn "VIP 资源 p_vip 未 Started，外部入口 ${_va} 不可用"
    fi
  fi

  if [ -b "$DRBD_VOLUME" ]; then
    local _dstat; _dstat="$(drbdadm status "$DRBD_RESOURCE" 2>/dev/null)"
    printf '%s\n' "$_dstat" | sed 's/^/    /'
    # Promoted 只说明本机盘 UpToDate；initial sync 未完成时对端仍落后，
    # 此刻切过去会丢数据，不具备本方案承诺的 RPO=0，必须显式提示。
    if printf '%s\n' "$_dstat" | grep -qE 'replication:Sync(Source|Target)|peer-disk:Inconsistent|out-of-sync:[[:space:]]*[1-9]|oos:[[:space:]]*[1-9]'; then
      wrn "DRBD 仍在同步（对端数据不完整），此刻不具备 RPO=0：请同步完成后复查 drbdadm status"
    else
      ok "DRBD 两侧数据已一致（无待同步数据）"
    fi
  fi

  printf "\n${C}== 检查汇总 ==${N}\n  PASS=%d WARN=%d FAIL=%d\n" "$PASS" "$WARN" "$FAIL"
  if [ "$FAIL" -gt 0 ]; then printf "  结论：存在 FAIL，请修复后再上线。\n"; CHECK_RC=1; else printf "  结论：部署检查通过。\n"; fi
  return "$CHECK_RC"
}

# ----------------------------- 防火墙 --------------------------------------
open_ports(){
  [ "$OPEN_PORTS" = 1 ] || return 0
  hdr "配置防火墙"
  local tcp="2224 $DRBD_PORT $MO_PORT" udp="5405 5404"
  if command -v firewall-cmd >/dev/null 2>&1; then
    runq "systemctl enable --now firewalld"
    for p in $tcp; do runq "firewall-cmd --permanent --add-port=$p/tcp"; done
    for p in $udp; do runq "firewall-cmd --permanent --add-port=$p/udp"; done
    runq "firewall-cmd --reload"
  elif command -v ufw >/dev/null 2>&1; then
    for p in $tcp; do runq "ufw allow $p/tcp"; done
    for p in $udp; do runq "ufw allow $p/udp"; done
  else
    info "未检测到 firewalld/ufw，跳过（请自行放行 2224/tcp,5405/udp,${DRBD_PORT}/tcp,${MO_PORT}/tcp）"
  fi
}

# ----------------------------- 主流程 --------------------------------------
run_stage(){
  # 不把阶段函数放在 if/! 或 &&/|| 的条件中，否则 Bash 会关闭函数内部的 errexit。
  if stage_enabled "$1"; then
    CURRENT_STAGE="$1"
    "stage_$1"
  fi
}
main(){
  validate_stages
  detect_os
  run_stage pre
  run_stage pkg
  # 防火墙必须先于 drbd/cluster：pcsd(2224/tcp)、corosync(5405/udp,5404/udp)、
  # DRBD($DRBD_PORT/tcp) 若被 firewalld/ufw 拦截，stage_cluster 等待 quorum
  # 必然超时失败。曾排在全部阶段之后，等于从未在集群组建前放行端口。
  # OPEN_PORTS 是独立开关(--no-open-ports 关闭)，且只在真正做部署的阶段才执行；
  # 纯 --stages check 复检或 teardown 不改动防火墙状态。
  local _fw=1 _s
  for _s in pre pkg docker moconf drbd cluster fence resources; do
    stage_enabled "$_s" && _fw=0
  done
  if [ "$_fw" = 0 ]; then CURRENT_STAGE=firewall; open_ports; fi
  run_stage docker
  run_stage moconf
  run_stage drbd
  run_stage cluster
  run_stage fence
  run_stage resources
  run_stage teardown
  run_stage check
  [ "$CHECK_RC" = 0 ] || die "部署检查失败，停止执行"
  # $SELF 来自 --self，不能直接当 printf 格式串（% / \ 会被解释）
  printf "\n${G}完成${N}：%s (%s)\n" "$SELF" "$([ "$BOOTSTRAP" = 1 ] && echo 引导节点 || echo 普通节点)"
  if [ "$BOOTSTRAP" != 1 ]; then info "提示：请在引导节点执行本脚本(加 --bootstrap)以组建集群并创建资源。"; fi
  return "$CHECK_RC"
}

main "$@"
