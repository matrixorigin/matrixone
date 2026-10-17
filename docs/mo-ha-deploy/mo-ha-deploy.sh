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
set -u

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
MO_CONFIG_DIR="${MO_CONFIG_DIR:-}"; NO_MO="${NO_MO:-0}"
DOCKER_MODE="${DOCKER_MODE:-official}"     # official | distro | skip
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
HACLUSTER_PW="${HACLUSTER_PW:-hacluster}"; OPEN_PORTS="${OPEN_PORTS:-1}"
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

run(){  if [ "$DRY_RUN" = 1 ]; then printf "    (dry-run) %s\n" "$*"; else info "exec: $*"; bash -c "$*"; fi; }
runq(){ if [ "$DRY_RUN" = 1 ]; then printf "    (dry-run) %s\n" "$*"; else bash -c "$*"; fi; }
stage_enabled(){ case ",$STAGES," in *",$1,"*) return 0;; *) return 1;; esac; }

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
  --mo-dir PATH          MO 运行目录(默认 /opt/mo-ha)
  --mo-hostname NAME     explicit-hostname(默认 mo-ha，两台必须一致)
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
  --fence-extra "k=v,k=v" --fence-delay-max N(默认 5)
  --fence-agent NAME     custom_http 场景的 agent 名
  --fence-addr-param KEY 带外地址参数名（默认 ipmi/redfish=ipaddr, custom_http=ip）
  --sbd-device /dev/sdX --watchdog /dev/watchdog

【行为控制】
  --drbd-src-url URL     DRBD 源码包(默认 LINBIT 9.2.20；内核模块包缺失时用于源码构建)
  --no-drbd-kernel-build 禁止源码构建 DRBD 内核模块(只用发行版/ELRepo 包)
  --config FILE          从文件读取上述变量(KEY=VALUE，CLI 覆盖文件)
  --bootstrap            引导节点(建集群/fencing/资源)；仅一台使用
  --format-drbd          首次格式化 DRBD(仅 bootstrap 一次)
  --drbd9-quorum         使用 DRBD9 quorum majority(需 kmod-drbd 9.x)
  --docker-mode M        official(默认)|distro|skip
  --no-pull-image        不在部署时预拉 MO 镜像(默认先 docker pull，避免启动超时)
  --hacluster-password P pcsd hacluster 密码(两台须一致)
  --stages LIST          pre,pkg,docker,moconf,drbd,cluster,fence,resources,check,teardown
  --teardown             等价于 --stages teardown(销毁集群/DRBD/MO，危险)
  --no-open-ports        不自动配置防火墙
  --dry-run              只打印不执行
  --yes                  免交互
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
    --mo-dir) MO_DIR="${2:-}"; shift 2;;
    --mo-hostname) MO_HOSTNAME="${2:-}"; shift 2;;
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

pkg_update(){ case "$OS_FAMILY" in rpm) runq "$PKG makecache -y || true";; deb) runq "$PKG update -y";; esac; }
pkg_install(){
  local pkgs="$*"; [ -n "$pkgs" ] || return 0
  case "$OS_FAMILY" in
    rpm) run "$PKG install -y $pkgs";;
    deb) pkg_update; run "DEBIAN_FRONTEND=noninteractive $PKG install -y $pkgs";;
  esac
}

# EL8/EL9: pacemaker/pcs/resource-agents 位于 highavailability 仓库，默认禁用
enable_ha_repo(){
  [ "$OS_FAMILY" = rpm ] || return 0
  runq "$PKG install -y dnf-plugins-core 2>/dev/null || true"
  runq "$PKG config-manager --set-enabled highavailability 2>/dev/null || \
        sed -i '/^\[highavailability\]/,/^\[/ s/^enabled=0/enabled=1/' /etc/yum.repos.d/*.repo 2>/dev/null || true"
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
  d="$(basename "$tb" .tar.gz)"
  run "cd /usr/src/$d && make -j\$(nproc) && make install && depmod -a"
}

# 确保 rpm 系有可加载的 drbd 内核模块
ensure_drbd_module_rpm(){
  lsmod 2>/dev/null | grep -q '^drbd' && { info "DRBD 内核模块已加载"; return 0; }
  runq "modprobe drbd 2>/dev/null || true"
  lsmod 2>/dev/null | grep -q '^drbd' && { info "DRBD 内核模块已加载"; return 0; }
  if [ "$DRY_RUN" = 1 ]; then info "(dry-run) 尝试 ELRepo kmod-drbd / 源码构建"; return 0; fi
  info "本机无 drbd 模块，尝试 ELRepo kmod-drbd ..."
  runq "$PKG install -y https://www.elrepo.org/elrepo-release-\$(rpm -E %rhel).elrepo.noarch.rpm || true"
  runq "$PKG install -y kmod-drbd9x || $PKG install -y kmod-drbd || true"
  runq "modprobe drbd 2>/dev/null || true"
  lsmod 2>/dev/null | grep -q '^drbd' && { info "ELRepo DRBD 内核模块安装成功"; return 0; }
  if [ "$DRBD_KERNEL_FALLBACK" = never ]; then
    wrn "DRBD 内核模块不可用(已用 --no-drbd-kernel-build 禁用源码构建)"
    return 1
  fi
  wrn "ELRepo 无匹配内核模块(常见于 aarch64)，改用源码构建"
  build_drbd_from_source
  runq "modprobe drbd 2>/dev/null || true"
  lsmod 2>/dev/null | grep -q '^drbd'
}

# ----------------------------- 阶段 pre -------------------------------------
stage_pre(){
  hdr "阶段 pre: 系统准备"
  [ "$(id -u)" = 0 ] || [ "$DRY_RUN" = 1 ] || die "请以 root 运行"
  [ -n "$SELF" ] && [ -n "$PEER" ] && [ -n "$SELF_IP" ] && [ -n "$PEER_IP" ] \
    || die "缺少参数: --self/--peer/--self-ip/--peer-ip"
  [ -n "$DRBD_DEVICE" ] || die "缺少 --drbd-device"
  command -v hostnamectl >/dev/null 2>&1 && run "hostnamectl set-hostname $SELF"
  run "sed -i '/# MO-HA-BEGIN/,/# MO-HA-END/d' /etc/hosts"
  run "printf '# MO-HA-BEGIN\n$SELF_IP $SELF\n$PEER_IP $PEER\n# MO-HA-END\n' >> /etc/hosts"
  run "printf 'net.ipv4.ip_nonlocal_bind=1\nvm.swappiness=10\n' > /etc/sysctl.d/90-mo-ha.conf"
  run "sysctl --system >/dev/null 2>&1 || true"
  command -v chronyc >/dev/null 2>&1 && runq "systemctl enable --now chronyd 2>/dev/null || systemctl enable --now chrony 2>/dev/null || true"
  info "主机名与 hosts 就绪: $SELF ($SELF_IP) <-> $PEER ($PEER_IP)"
}

# ----------------------------- 阶段 pkg -------------------------------------
stage_pkg(){
  hdr "阶段 pkg: 安装依赖"
  pkg_update
  case "$OS_FAMILY" in
    rpm)
      enable_ha_repo
      runq "$PKG install -y epel-release || true"
      pkg_install "drbd-utils drbd-pacemaker pacemaker corosync pcs resource-agents fence-agents-all"
      runq "modprobe drbd 2>/dev/null || true"
      ensure_drbd_module_rpm || true
      ;;
    deb)
      pkg_install "drbd-utils pacemaker corosync pcs fence-agents resource-agents-base resource-agents-common resource-agents-extra"
      # drbd 内核模块在 linux-modules-extra，必须与当前运行内核版本匹配
      runq "DEBIAN_FRONTEND=noninteractive apt-get install -y linux-modules-extra-\$(uname -r) 2>/dev/null || true"
      runq "depmod -a 2>/dev/null || true"
      runq "modprobe drbd 2>/dev/null || true"
      ;;
  esac
  ensure_ocf_agents
  run "printf 'drbd\n' > /etc/modules-load.d/drbd.conf"
  runq "modprobe drbd || true"
  if [ "$DRY_RUN" != 1 ] && ! lsmod 2>/dev/null | grep -q '^drbd'; then
    wrn "DRBD 内核模块仍未加载：请安装与 \$(uname -r) 匹配的内核模块包"
    wrn "  deb: linux-modules-extra-\$(uname -r) | rpm: ELRepo kmod-drbd 或 --drbd-src-url 源码构建"
  fi
  [ "$DRY_RUN" = 1 ] || drbdadm --version 2>/dev/null | sed 's/^/    /' || true
}

# ----------------------------- 阶段 docker ----------------------------------
compose_wrapper_path(){
  # ocf:heartbeat:docker-compose 默认调 /usr/bin/docker-compose
  echo "/usr/bin/docker-compose"
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
      case "$OS_FAMILY" in
        rpm)
          runq "$PKG install -y dnf-plugins-core || true"
          runq "$PKG config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo || true"
          pkg_install "docker-ce docker-ce-cli containerd.io docker-compose-plugin";;
        deb)
          pkg_install "ca-certificates curl gnupg"
          runq "install -m0755 -d /etc/apt/keyrings"
          runq "curl -fsSL https://download.docker.com/linux/\$(. /etc/os-release; echo \$ID)/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg || true"
          runq "echo \"deb [arch=\$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/\$(. /etc/os-release; echo \$ID) \$(. /etc/os-release; echo \$VERSION_CODENAME) stable\" > /etc/apt/sources.list.d/docker.list || true"
          pkg_install "docker-ce docker-ce-cli containerd.io docker-compose-plugin";;
      esac;;
    *) die "--docker-mode 只能是 official|distro|skip";;
  esac
  runq "systemctl enable --now docker || true"
  # compose wrapper（存在 docker compose 插件时）
  if [ "$DRY_RUN" = 1 ] || [ -x /usr/bin/docker ]; then
    run "cat > $(compose_wrapper_path) <<'WRAP'
#!/bin/sh
exec /usr/bin/docker compose \"\$@\"
WRAP
chmod 0755 $(compose_wrapper_path)"
  fi
  run "mkdir -p $DATA_DIR $MO_DIR/conf"
  # 预拉 MO 镜像（避免 docker-compose RA start 超时触发资源迁移）
  if [ "$NO_MO" != 1 ] && [ "$PULL_IMAGE" = 1 ]; then
    info "预拉取 MO 镜像: $MO_IMAGE（首次较慢，可用 --no-pull-image 跳过）"
    if runq "docker pull $MO_IMAGE"; then
      info "镜像已就绪: $MO_IMAGE"
    else
      wrn "镜像预拉取失败，请确认网络/代理；也可稍后手动 docker pull $MO_IMAGE"
    fi
  fi
  if command -v getenforce >/dev/null 2>&1 && [ "$(getenforce 2>/dev/null)" != "Disabled" ]; then
    runq "semanage fcontext -a -t container_file_t '$DATA_DIR(/.*)?' 2>/dev/null || restorecon -R '$DATA_DIR' 2>/dev/null || true"
  fi
}

# ----------------------------- 阶段 moconf ----------------------------------
build_shared_block(){
  if [ -n "$S3_BUCKET" ]; then
    cat <<S3
[[fileservice]]
name = "SHARED"
backend = "S3"

[fileservice.s3]
endpoint     = "${S3_ENDPOINT}"
bucket       = "${S3_BUCKET}"
region       = "${S3_REGION}"
key-prefix   = "${S3_KEY_PREFIX}"
key-id       = "${S3_KEY_ID}"
key-secret   = "${S3_KEY_SECRET}"
S3
  else
    cat <<'LOCAL'
[[fileservice]]
name = "SHARED"
backend = "DISK-V2"
data-dir = "./mo-data/shared2"
LOCAL
  fi
}
stage_moconf(){
  hdr "阶段 moconf: 下发 MatrixOne 配置"
  if [ "$NO_MO" = 1 ]; then info "NO_MO=1，跳过"; return 0; fi
  local payload="$MO_CONFIG_DIR"
  [ -n "$payload" ] || payload="$(cd "$(dirname "$0")" && pwd)/mo-config"
  if [ ! -d "$payload" ]; then
    wrn "未找到配置目录 $payload"
    info "请手动将 docker-compose.yaml 与 conf/ 放入 $MO_DIR（或用 --mo-config-dir 指定）"
    return 0
  fi
  run "mkdir -p $MO_DIR/conf"
  run "cp -a '$payload/.' $MO_DIR/"

  if [ "$DRY_RUN" = 1 ]; then
    printf "    (dry-run) 替换 compose 占位符 + 生成 SHARED fileservice(%s)\n" \
      "$([ -n "$S3_BUCKET" ] && echo S3 || echo DISK-V2)"
    return 0
  fi
  sed -i "s#__MO_IMAGE__#${MO_IMAGE}#g; s#__MO_PORT__#${MO_PORT}#g; s#__DATA_DIR__#${DATA_DIR}#g; s#__MO_DIR__#${MO_DIR}#g" "$MO_DIR/docker-compose.yaml"
  sed -i "s#__MO_HOSTNAME__#${MO_HOSTNAME}#g" "$MO_DIR/conf/log.toml" 2>/dev/null || true
  local blk; blk="$(mktemp)"; build_shared_block > "$blk"
  for f in "$MO_DIR/conf/tn.toml" "$MO_DIR/conf/cn.toml"; do
    [ -f "$f" ] || continue
    awk -v blkfile="$blk" '
      /__SHARED_FILESERVICE__/ { while ((getline l < blkfile) > 0) print l; next } { print }
    ' "$f" > "$f.tmp" && mv "$f.tmp" "$f"
  done
  rm -f "$blk"
  if [ -f "$MO_DIR/conf/launch.toml" ]; then ok "已下发 $MO_DIR (compose + conf)"; else wrn "$MO_DIR/conf/launch.toml 缺失"; fi
}

# ----------------------------- 阶段 drbd ------------------------------------
stage_drbd(){
  hdr "阶段 drbd: 配置 DRBD"
  [ -b "$DRBD_DEVICE" ] || [ "$DRY_RUN" = 1 ] || die "$DRBD_DEVICE 不是块设备"
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
  }
  disk {
    on-io-error detach;
  }${quorum_opts}
}
DRBD"
  runq "drbdadm create-md ${DRBD_RESOURCE} --force 2>&1 | tail -3 || true"

  if [ "$DRY_RUN" != 1 ] && [ "$FORMAT_DRBD" = 1 ]; then
    if [ "$BOOTSTRAP" != 1 ]; then
      wrn "--format-drbd 只应在 bootstrap 节点执行；本机跳过格式化"
    else
      info "首次初始化 DRBD 数据卷 (up -> promote --force -> mkfs.xfs -> down)"
      runq "drbdadm up ${DRBD_RESOURCE}"
      if blkid "$DRBD_VOLUME" >/dev/null 2>&1; then
        info "$DRBD_VOLUME 已有文件系统，跳过 mkfs"
      else
        runq "drbdadm primary ${DRBD_RESOURCE} --force"
        runq "mkfs.xfs -f ${DRBD_VOLUME}"
      fi
      runq "drbdadm down ${DRBD_RESOURCE}"
      ok "DRBD 数据卷就绪，交由 Pacemaker 管理"
    fi
  elif [ "$DRY_RUN" != 1 ] && [ "$BOOTSTRAP" = 1 ]; then
    wrn "未指定 --format-drbd：若 $DRBD_VOLUME 尚未格式化，集群启动 p_fs 会失败"
  fi
  info "DRBD 资源 ${DRBD_RESOURCE}(${DRBD_VOLUME} <- ${DRBD_DEVICE}) 配置完成"
}

# ----------------------------- 阶段 cluster --------------------------------
stage_cluster(){
  hdr "阶段 cluster: corosync/pacemaker"
  runq "systemctl enable --now pcsd || true"
  runq "echo 'hacluster:${HACLUSTER_PW}' | chpasswd"
  runq "mkdir -p /etc/corosync /var/lib/corosync /var/log/corosync"
  runq "systemctl enable corosync pacemaker 2>/dev/null || true"
  if [ "$BOOTSTRAP" != 1 ]; then
    info "非引导节点：pcsd/hacluster 已就绪，等待引导节点组建集群"
    return 0
  fi
  runq "pcs host auth $SELF $PEER -u hacluster -p '${HACLUSTER_PW}'"
  # pcs cluster setup 只会分发已存在的 corosync authkey，缺失时需先生成
  if [ "$DRY_RUN" != 1 ]; then
    runq "mkdir -p /etc/corosync /var/lib/corosync /var/log/corosync"
    [ -f /etc/corosync/authkey ] || runq "corosync-keygen -l"
  fi
  runq "pcs cluster setup $CLUSTER $SELF $PEER --start --enable --force"
  runq "pcs cluster enable --all"
  info "等待集群形成 quorum..."
  if [ "$DRY_RUN" != 1 ]; then
    _q=0
    for _i in $(seq 1 40); do
      if corosync-quorumtool -s 2>/dev/null | grep -qE "Quorate:[[:space:]]+Yes"; then _q=1; break; fi
      sleep 2
    done
    [ "$_q" = 1 ] && ok "quorum 已形成" || wrn "等待 quorum 超时，请检查 corosync 与 /etc/hosts"
  fi
  runq "pcs property set stonith-enabled=true"
  runq "pcs property set no-quorum-policy=stop"
  [ "$FENCE_SCENARIO" = "sbd" ] && runq "pcs property set have-watchdog=true"
  info "集群 $CLUSTER 组建完成"
}

# ----------------------------- 阶段 fence ----------------------------------
fence_agent_for(){
  case "$FENCE_SCENARIO" in
    ipmi) echo fence_ipmilan;; redfish) echo fence_redfish;;
    pdu) echo fence_apc_snmp;; vmware) echo fence_vmware_rest;;
    proxmox) echo fence_proxmox_ve;; virsh) echo fence_virsh;;
    aws) echo fence_aws;; azure) echo fence_azure_arm;;
    gce) echo fence_gce;; aliyun) echo fence_aliyun;;
    custom_http) echo "${FENCE_AGENT_OVERRIDE:-fence_lima}";;
    sbd) echo external/sbd;;
    *) die "未知 fencing 场景: $FENCE_SCENARIO";;
  esac
}
extra_params(){ [ -n "$FENCE_EXTRA" ] && echo "$FENCE_EXTRA" | tr ',' ' ' || echo ""; }

stage_fence(){
  hdr "阶段 fence: STONITH ($FENCE_SCENARIO)"
  [ -n "$FENCE_SCENARIO" ] || die "缺少 --fence-scenario"
  [ "$BOOTSTRAP" = 1 ] || { info "非引导节点：跳过 fencing 创建"; return 0; }
  local agent; agent="$(fence_agent_for)"; local ex; ex="$(extra_params)"
  local addrkey="${FENCE_ADDR_PARAM}"
  if [ -z "$addrkey" ]; then
    case "$FENCE_SCENARIO" in custom_http) addrkey=ip;; *) addrkey=ipaddr;; esac
  fi

  if [ "$FENCE_SCENARIO" = "sbd" ]; then
    [ -n "$SBD_DEVICE" ] || die "sbd 场景需要 --sbd-device"
    run "cat > /etc/sysconfig/sbd <<SBD
SBD_DEVICE=\"${SBD_DEVICE}\"
SBD_PACEMAKER=yes
SBD_STARTMODE=always
SBD_WATCHDOG_DEV=${WATCHDOG}
SBD_WATCHDOG_TIMEOUT=5
SBD"
    runq "systemctl enable --now sbd || true"
    runq "pcs stonith create fence_${DRBD_RESOURCE} external/sbd pcmk_delay_max=${FENCE_DELAY_MAX} op monitor interval=30s"
    return 0
  fi

  case "$FENCE_SCENARIO" in
    ipmi)
      [ -n "$FENCE_TARGET_SELF" ] && [ -n "$FENCE_TARGET_PEER" ] || die "ipmi 需要 --fence-target-self/peer"
      runq "pcs stonith create fence_$SELF fence_ipmilan $addrkey=$FENCE_TARGET_SELF login=$FENCE_LOGIN passwd='$FENCE_PASSWORD' $ex pcmk_host_list=$SELF pcmk_delay_max=$FENCE_DELAY_MAX op monitor interval=30s"
      runq "pcs stonith create fence_$PEER fence_ipmilan $addrkey=$FENCE_TARGET_PEER login=$FENCE_LOGIN passwd='$FENCE_PASSWORD' $ex pcmk_host_list=$PEER pcmk_delay_max=$FENCE_DELAY_MAX op monitor interval=30s";;
    redfish)
      [ -n "$FENCE_TARGET_SELF" ] && [ -n "$FENCE_TARGET_PEER" ] || die "redfish 需要 --fence-target-self/peer"
      runq "pcs stonith create fence_$SELF fence_redfish $addrkey=$FENCE_TARGET_SELF username=$FENCE_LOGIN password='$FENCE_PASSWORD' $ex pcmk_host_list=$SELF pcmk_delay_max=$FENCE_DELAY_MAX op monitor interval=30s"
      runq "pcs stonith create fence_$PEER fence_redfish $addrkey=$FENCE_TARGET_PEER username=$FENCE_LOGIN password='$FENCE_PASSWORD' $ex pcmk_host_list=$PEER pcmk_delay_max=$FENCE_DELAY_MAX op monitor interval=30s";;
    custom_http)
      [ -n "$FENCE_TARGET_SELF" ] && [ -n "$FENCE_TARGET_PEER" ] || die "custom_http 需要 --fence-target-self/peer"
      runq "pcs stonith create fence_$SELF $agent $addrkey=$FENCE_TARGET_SELF $ex pcmk_host_list=$SELF pcmk_delay_max=$FENCE_DELAY_MAX op monitor interval=30s"
      runq "pcs stonith create fence_$PEER $agent $addrkey=$FENCE_TARGET_PEER $ex pcmk_host_list=$PEER pcmk_delay_max=$FENCE_DELAY_MAX op monitor interval=30s";;
    *)
      [ -n "$FENCE_PLUG_SELF" ] && [ -n "$FENCE_PLUG_PEER" ] || die "$FENCE_SCENARIO 需要 --fence-plug-self/peer"
      local map="$SELF:$FENCE_PLUG_SELF;$PEER:$FENCE_PLUG_PEER" ipar=""
      case "$FENCE_SCENARIO" in
        pdu) [ -n "$FENCE_SHARED_IP" ] || die "pdu 需要 --fence-shared-ip"; ipar="ipaddr=$FENCE_SHARED_IP login=$FENCE_LOGIN passwd='$FENCE_PASSWORD' snmp_community=$FENCE_SNMP";;
        vmware|proxmox) [ -n "$FENCE_SHARED_IP" ] || die "$FENCE_SCENARIO 需要 --fence-shared-ip"; ipar="ipaddr=$FENCE_SHARED_IP login=$FENCE_LOGIN passwd='$FENCE_PASSWORD'";;
        virsh) [ -n "$FENCE_SHARED_IP" ] || die "virsh 需要 --fence-shared-ip"; ipar="ipaddr=$FENCE_SHARED_IP login=$FENCE_LOGIN";;
      esac
      runq "pcs stonith create fence_$DRBD_RESOURCE $agent $ipar $ex pcmk_host_map='$map' pcmk_delay_max=$FENCE_DELAY_MAX op monitor interval=30s";;
  esac
}

# ----------------------------- 阶段 resources ------------------------------
stage_resources(){
  hdr "阶段 resources: DRBD/FS/MatrixOne 资源"
  [ "$BOOTSTRAP" = 1 ] || { info "非引导节点：跳过资源创建"; return 0; }
  runq "pcs resource create p_drbd ocf:linbit:drbd drbd_resource=$DRBD_RESOURCE op monitor interval=20s role=Promoted timeout=30s op monitor interval=30s role=Unpromoted timeout=30s"
  runq "pcs resource promotable p_drbd"
  # linbit drbd RA 要求显式 clone-max<=2，并需要 clone 通知
  runq "pcs resource meta p_drbd-clone clone-max=2 notify=true"
  runq "pcs resource create p_fs Filesystem device=$DRBD_VOLUME directory=$DATA_DIR fstype=xfs op monitor interval=20s"
  runq "pcs constraint colocation add p_fs with p_drbd-clone INFINITY with-rsc-role=Promoted"
  runq "pcs constraint order promote p_drbd-clone then start p_fs"
  if [ "$NO_MO" != 1 ]; then
    runq "mkdir -p $MO_DIR"
    runq "pcs resource create p_mo ocf:heartbeat:docker-compose binpath=$(compose_wrapper_path) dirpath=$MO_DIR ymlfile=docker-compose.yaml op monitor interval=30s timeout=60s op start timeout=$MO_START_TIMEOUT interval=0 op stop timeout=120s"
    runq "pcs constraint colocation add p_mo with p_fs INFINITY"
    runq "pcs constraint order start p_fs then start p_mo"
  fi
  if [ -n "$VIP" ]; then
    runq "pcs resource create p_vip IPaddr2 ip=${VIP%%/*} cidr_netmask=${VIP##*/} op monitor interval=20s"
    runq "pcs constraint colocation add p_vip with p_fs INFINITY"
    runq "pcs constraint order start p_fs then start p_vip"
  fi
}

# ----------------------------- 阶段 teardown -------------------------------
stage_teardown(){
  hdr "阶段 teardown: 清理既有 HA 配置 (危险)"
  info "将销毁集群、DRBD、MO 容器与数据目录挂载（保留 $DRBD_DEVICE 数据需自行 backup）"
  runq "pcs cluster stop --all 2>/dev/null || true"
  runq "pcs cluster destroy --all 2>/dev/null || true"
  runq "systemctl disable --now corosync pacemaker pcsd sbd 2>/dev/null || true"
  runq "docker rm -f matrixone 2>/dev/null || true"
  runq "drbdadm down $DRBD_RESOURCE 2>/dev/null || true"
  runq "drbdadm wipe-md $DRBD_RESOURCE 2>/dev/null || true"
  runq "wipefs -a $DRBD_DEVICE 2>/dev/null || true"
  runq "umount $DATA_DIR 2>/dev/null || true"
  runq "rm -f /etc/drbd.d/$DRBD_RESOURCE.res /etc/modules-load.d/drbd.conf"
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
  if ! command -v pcs >/dev/null 2>&1; then wrn "无 pcs，无法检查"; return 0; fi

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
  # 节点覆盖
  local cfg; cfg="$(pcs stonith config 2>/dev/null | tr '\n' ' ')"
  for n in "$SELF" "$PEER"; do
    if printf '%s' "$cfg" | grep -qE "$n"; then ok "节点 $n 已被 fencing 覆盖"; else bad "节点 $n 未被 fencing 覆盖"; fi
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
  fi
  if [ -b "$DRBD_VOLUME" ]; then drbdadm status "$DRBD_RESOURCE" 2>/dev/null | sed 's/^/    /'; fi

  printf "\n${C}== 检查汇总 ==${N}\n  PASS=%d WARN=%d FAIL=%d\n" "$PASS" "$WARN" "$FAIL"
  if [ "$FAIL" -gt 0 ]; then printf "  结论：存在 FAIL，请修复后再上线。\n"; CHECK_RC=1; else printf "  结论：部署检查通过。\n"; fi
}

# ----------------------------- 防火墙 --------------------------------------
open_ports(){
  [ "$OPEN_PORTS" = 1 ] || return 0
  hdr "配置防火墙"
  local tcp="2224 7788 7789 $MO_PORT" udp="5405 5404"
  if command -v firewall-cmd >/dev/null 2>&1; then
    runq "systemctl enable --now firewalld || true"
    for p in $tcp; do runq "firewall-cmd --permanent --add-port=$p/tcp || true"; done
    for p in $udp; do runq "firewall-cmd --permanent --add-port=$p/udp || true"; done
    runq "firewall-cmd --reload || true"
  elif command -v ufw >/dev/null 2>&1; then
    for p in $tcp; do runq "ufw allow $p/tcp || true"; done
    for p in $udp; do runq "ufw allow $p/udp || true"; done
  else
    info "未检测到 firewalld/ufw，跳过（请自行放行 2224/tcp,5405/udp,7789/tcp,${MO_PORT}/tcp）"
  fi
}

# ----------------------------- 主流程 --------------------------------------
main(){
  detect_os
  stage_enabled pre && stage_pre
  stage_enabled pkg && stage_pkg
  stage_enabled docker && stage_docker
  stage_enabled moconf && stage_moconf
  stage_enabled drbd && stage_drbd
  stage_enabled cluster && stage_cluster
  stage_enabled fence && stage_fence
  stage_enabled resources && stage_resources
  stage_enabled teardown && stage_teardown
  stage_enabled check && open_ports
  if stage_enabled check; then stage_check; fi
  printf "\n${G}完成${N}：$SELF ($([ "$BOOTSTRAP" = 1 ] && echo 引导节点 || echo 普通节点))\n"
  [ "$BOOTSTRAP" != 1 ] && info "提示：请在引导节点执行本脚本(加 --bootstrap)以组建集群并创建资源。"
  return "$CHECK_RC"
}

main "$@"
