# MO 整体架构

## 核心组件

MO 采用存算分离架构，由 4 个核心服务组成：

| 组件 | 包路径 | 职责 |
|------|--------|------|
| **CN (Compute Node)** | `pkg/cnservice/` | SQL 解析、计划生成、编译执行。无状态，可水平扩展 |
| **TN (Transaction Node)** | `pkg/tnservice/` | 事务处理、数据持久化、WAL 管理 |
| **LogService** | `pkg/logservice/` | 分布式日志（基于 Dragonboat/Raft），提供数据一致性保证 |
| **Proxy** | `pkg/proxy/` | 连接路由、负载均衡，将客户端请求分发到不同 CN |

## 辅助组件

| 组件 | 包路径 | 职责 |
|------|--------|------|
| **HAKeeper** | `pkg/hakeeper/` | 集群编排与健康检查，基于 Raft 状态机 |
| **ClusterService** | `pkg/clusterservice/` | 集群拓扑管理、节点发现 |
| **Gossip** | `pkg/gossip/` | 节点间元数据传播（基于 memberlist） |
| **TaskService** | `pkg/taskservice/` | 后台任务调度（CDC、自增 ID 等） |
| **Bootstrap** | `pkg/bootstrap/` | 集群初始化、版本管理 |

## 启动流程

入口：`cmd/mo-service/main.go` → `launch.go`

```
启动 → 读配置 → 启动 LogService → 启动 TN → 启动 CN → 启动 Proxy
```

配置文件位于 `etc/launch/`：
- `launch.toml` — 集群协调配置
- `cn.toml` — CN 配置（端口、引擎类型）
- `tn.toml` — TN 配置
- `log.toml` — LogService 配置

## 数据流

```
客户端 → Proxy → CN (SQL解析/计划/编译/执行)
                  ↕
                DisttaE (分布式引擎) ←→ TN (事务/存储)
                  ↕                      ↕
               FileService          LogService
              (对象存储)            (WAL/Raft)
```
