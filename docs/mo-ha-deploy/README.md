# MatrixOne 双机主备部署（DRBD + Pacemaker/STONITH）

两台机器，用 DRBD 块级复制整个数据目录，Pacemaker 保证严格单活。
MatrixOne 以 all-in-one 容器运行，由 Pacemaker 的 `docker-compose` 资源代理启停
（**不进 systemd**）。同步复制的 RPO=0 以 DRBD 正常连接、双端同步及隔离机制通过验收为前提；切换时间受故障检测、隔离和 MO 恢复耗时影响。

支持 RHEL / CentOS / Rocky / AlmaLinux 与 Debian / Ubuntu。

## 一、准备

两台机器各自准备：

- 一块**同容量裸盘**（下文以 `/dev/vdb` 为例）给 DRBD，**盘上数据会被清空**；必须是**专用空盘**，
  脚本会拒绝「该盘或其分区已挂载」的设备，避免把系统盘误写成数据盘
- 两机互通、时间同步、root 权限
- 能对**对端**强制断电的带外通道：IPMI / Redfish、PDU、虚拟化平台、云 API 或 SBD 共享盘
- 每台内存建议 ≥ 8GB

## 二、部署

把本目录拷到两台机器（下文以 `/root/mo-ha-deploy` 为例），准备参数文件：

```bash
cd /root/mo-ha-deploy
cp deploy.env.example node1.env     # 在 node2 上复制为 node2.env
chmod 600 node1.env      # 含凭据；且该文件会被 root source，务必仅用可信来源
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

隔离代理演练（对**非活动**节点重启，验证 fencing 和重新入群；不代替活动节点故障接管测试）：

```bash
sudo stonith_admin --reboot <非活动节点>
sudo pcs stonith history
sudo pcs status
```

预期被隔离节点重新上电、集群服务启动并恢复通信后重新入群，DRBD 重新同步到双端 `UpToDate`。活动节点断网测试还应验收：对端完成隔离并接管，恢复网卡后原节点无需手动启动即可回到 `Online`，DRBD 补齐数据，始终只有一个 Primary。业务可继续留在接管节点。

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

# Proxmox：VMID 映射到节点；显式填写当前 agent 要求的 vmtype
FENCE_SCENARIO="proxmox"
FENCE_SHARED_IP="pve.example.com"; FENCE_PLUG_SELF="101"; FENCE_PLUG_PEER="102"
FENCE_LOGIN="hauser@pve"; FENCE_PASSWORD="..."
FENCE_EXTRA="ipport=8006,vmtype=qemu"
```


fencing 不能省，也不能用假实现。上线前务必对非活动节点实弹演练一次。

## 五、注意

- 不要给 MatrixOne 建 systemd 服务，容器由 Pacemaker 管理。
- **默认口令风险**：MO 首次启动的 root 密码是 `111`，端口默认绑在所有网卡上。上线前务必改掉：`mysql -h127.0.0.1 -P6001 -uroot -p111 -e "alter user root identified by '<新密码>'"`；同时把 `MO_BIND_IP` 换成本机内网 IP（若填 VIP，必须同时配置 `VIP=`，脚本会让 VIP 先于容器启动），不要留 `0.0.0.0`。
- `--hacluster-password` 没有默认值，两台必须一致；密码只能用字母/数字与 `._@%+=,:/_-`，含引号/空白/`$` 等会被脚本直接拒绝。
- `--teardown` 会 `wipefs` DRBD 裸盘，属破坏性操作，必须显式加 `--yes`（或交互输入 `yes`）才会执行；
  执行前会校验该盘（及其分区）没有挂载点，避免抹掉系统盘。
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
| `backups/20261105-before-fixes/` | 本次修改前的原脚本、env 模板及 SHA-256 校验值 |
| `backups/20261105-before-proxmox-fix/` | 集成 Proxmox 自动发现修复前的脚本 |


备份与修改前原件已逐字节校验。若需回退仓库交付文件，将备份中的 `mo-ha-deploy.sh` 和 `deploy.env.example` 复制回本目录；这只回退文件，不会回滚已执行的系统/集群操作。

相关官方说明：[pcs 离线配置与差异提交](https://docs.redhat.com/en/documentation/red_hat_enterprise_linux/9/html/configuring_and_managing_high_availability_clusters/assembly_pcs-operation-configuring-and-managing-high-availability-clusters)、[Pacemaker DRBD 克隆参数示例](https://github.com/ClusterLabs/pacemaker/blob/main/doc/sphinx/Pacemaker_Administration/pcs-crmsh.rst)、[fence_pve 代理参数](https://github.com/ClusterLabs/fence-agents/blob/main/agents/pve/fence_pve.py)。
