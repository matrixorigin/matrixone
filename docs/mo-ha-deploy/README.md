# MatrixOne 双机主备部署（DRBD + Pacemaker/STONITH）

两台机器，用 DRBD 块级复制整个数据目录，Pacemaker 保证严格单活。
MatrixOne 以 all-in-one 容器运行，由 Pacemaker 的 `docker-compose` 资源代理启停
（**不进 systemd**）。RPO = 0，实测切换 12–18s。

支持 RHEL / CentOS / Rocky / AlmaLinux 与 Debian / Ubuntu。

## 一、准备

两台机器各自准备：

- 一块**同容量裸盘**（下文以 `/dev/vdb` 为例）给 DRBD，**盘上数据会被清空**
- 两机互通、时间同步、root 权限
- 能对**对端**强制断电的带外通道：IPMI / Redfish、PDU、虚拟化平台、云 API 或 SBD 共享盘
- 每台内存建议 ≥ 8GB

## 二、部署

把本目录拷到两台机器（下文以 `/root/mo-ha-deploy` 为例），准备参数文件：

```bash
cd /root/mo-ha-deploy
cp deploy.env.example node1.env     # 在 node2 上复制为 node2.env
chmod 600 node1.env
vi node1.env    # 至少改 SELF / PEER / SELF_IP / PEER_IP / DRBD_DEVICE / HACLUSTER_PW / fencing 参数
```

**顺序很重要：先非引导节点，再引导节点。**

```bash
# ① node2（非引导节点）：装包、Docker、配置、DRBD 元数据、加入集群
sudo ./mo-ha-deploy.sh --config node2.env \
  --stages pre,pkg,docker,moconf,drbd,cluster --yes

# ② node1（引导节点）：以上全部 + 建集群 + 格式化 DRBD + fencing + 资源 + 自检
sudo ./mo-ha-deploy.sh --config node1.env --bootstrap --format-drbd --yes
```

建议先加 `--dry-run` 看一遍要执行的命令。`--format-drbd` **只在首次部署时用**。

部署按阶段执行，可单独重跑某一阶段：

```bash
sudo ./mo-ha-deploy.sh --config node1.env --stages check --yes
```

## 三、验证

```bash
sudo pcs status          # 两节点 Online；p_drbd Promoted 在一侧；p_fs / p_mo 与它同侧
sudo drbdadm status mo   # 活动侧 Primary，对侧 peer-disk:UpToDate
mysql -h 127.0.0.1 -P 6001 -u root -p111 -e "select 1"
```

故障演练（对**非活动**节点断电，确认自动切换且数据不丢）：

```bash
sudo stonith_admin --reboot <非活动节点>
sudo pcs stonith history
sudo pcs status
```

被断电的节点重新上电后会自己回到集群，DRBD 自动重新同步到 `UpToDate`。

## 四、fencing 参数

最常用的三种，其余场景见 `deploy.env.example` 里的注释。

```bash
# 物理机 BMC：每台一个带外地址
FENCE_SCENARIO="ipmi"
FENCE_TARGET_SELF="10.0.0.201"      # 本机 BMC
FENCE_TARGET_PEER="10.0.0.202"      # 对端 BMC
FENCE_LOGIN="admin"; FENCE_PASSWORD="..."

# PDU：单设备，用插座号区分
FENCE_SCENARIO="pdu"
FENCE_SHARED_IP="10.0.0.50"; FENCE_PLUG_SELF="3"; FENCE_PLUG_PEER="4"

# 虚拟化平台：单入口，用 VM 标识区分
FENCE_SCENARIO="vmware"
FENCE_SHARED_IP="vcenter.lab"; FENCE_PLUG_SELF="vm-101"; FENCE_PLUG_PEER="vm-102"
```

fencing 不能省，也不能用假实现。上线前务必对非活动节点实弹演练一次。

## 五、注意

- 不要给 MatrixOne 建 systemd 服务，容器由 Pacemaker 管理。
- `--hacluster-password` 没有默认值，两台必须一致；密码只能用字母/数字与 `._@%+=,:/_-`，含引号/空白/`$` 等会被脚本直接拒绝。
- `--teardown` 会 `wipefs`  DRBD 裸盘，属破坏性操作，必须显式加 `--yes`（或交互输入 `yes`）才会执行。
- 不要 `systemctl enable drbd`，它与 Pacemaker 冲突；模块靠 `/etc/modules-load.d/drbd.conf` 加载。
- 脚本会先 `docker pull` 镜像再建资源，避免 `p_mo` 启动超时导致资源误切换。
- 纯双节点在网络分区时两侧都会停写（安全但保守），生产建议加 witness / 仲裁盘。
- 改完 IP、扩容、换盘之后，用 `--stages check` 复检一遍。

## 六、文件

| 文件 | 说明 |
| --- | --- |
| `mo-ha-deploy.sh` | 一键部署脚本，RPM / DEB 通用 |
| `deploy.env.example` | 参数模板，复制后修改 |
| `mo-config/` | MatrixOne all-in-one compose 与 launch/log/tn/cn 配置模板 |
| `agents/heartbeat/docker-compose` | OCF 资源代理（EL9 的 resource-agents 不含，随包下发） |
