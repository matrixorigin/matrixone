# Kafka Connect 与 NiFi 经 Kafka 的 MO 原子事件 sink

本配置对应 [#29857](https://github.com/matrixorigin/matrixone/issues/29857)。
Kafka Connect 3.9 的 `AtomicSinkConnector` 将事件账本、非幂等计数变更和
MO checkpoint 放在同一事务。NiFi 2.4 使用 `PublishKafka` 把同样的不可变
事件送入 Kafka，再由该 Connect sink 写入 MO。NiFi 的 `success` 只表示
Kafka 已接收；下游 MO 完成情况须查询账本、位点和 connector 状态。本配置
不把通用 JDBC sink、NiFi `PutSQL` 或端到端跨系统事务称作 exactly-once。

## 1. 准备数据库与事件

在专用数据库执行 [`schema.sql`](../../connectors/mo-eos-sink/sql/schema.sql)。
示例建立 `orders` scope、`mo_eos_events` topic 的分区 0。多分区时，
在启动 connector 之前为每个分区插入 `(scope, topic, partition, -1, 0)`
checkpoint。scope/topic/event ID 用 UTF-8 原始字节存入 `VARBINARY`，
checkpoint 的 `topic_id` 在首次认领时绑定 Kafka broker 返回的 topic ID
（`Uuid.toString()` 的 22 个 ASCII 字节）。只有 `last_offset=-1` 且
`owner_epoch=0` 的新 checkpoint 可自动绑定；已有进度但没有 topic ID
会停止，须人工核对原 topic 身份后迁移，无法证明时改用新 scope 从可信源重放。
同名 topic 删除重建后 ID 不同，插件会在业务 SQL 前停止，不能沿用旧位点。
不要替换为默认排序规则下的 `VARCHAR`。`mo_sink_budget` 限制账本逻辑
行数与字节数；到阈值时任务停下并保留全部去重证据，不自动清理。

每条 Kafka 消息的 value 必须是一个 UTF-8 JSON 对象，例如：

```json
{"version":1,"event_id":"source-operation-42","operation":"increment","delta":7,"payload":"订单确认"}
```

只支持这五个字段及 `increment` 操作。`event_id` 必须由源业务操作稳定
生成并在重试时保留；不可使用每次重试的新 UUID、Kafka offset 或可反复
更新的目标行主键。`delta` 是精确的有符号 int64 整数。删除、主键变更、
schema evolution、缺失 ID、不同 payload 的同 ID、Kafka tombstone 和
未知字段会停止分区，且不会推进 MO checkpoint。当前业务效果为
`mo_sink_totals.n += delta`；其他业务表需要适配器新版本与相应测试。

适配器自行计算 `mo-event-frame-v1` SHA-256。帧依次为 ASCII `MOE1`、
scope/event ID/operation/payload 的 4 字节大端长度及 UTF-8 内容，再加
8 字节大端有符号 delta。JSON 字段顺序与空白不影响摘要；Unicode 不做
规范化。相同 ID、相同摘要重放只推进位点，不再执行计数变更。

## 2. 部署 Kafka Connect

在 [`connectors/mo-eos-sink`](../../connectors/mo-eos-sink) 运行
`mvn -DskipTests package`，将 `target/mo-eos-sink-0.1.0.jar` 单独放入
Connect worker 的 `plugin.path` 子目录，再重启 worker。该 jar 包含
MySQL Connector/J 与 Jackson，Kafka Connect API 由 worker 提供。
worker 必须允许运行 connector 的 consumer 配置覆盖，例如
`connector.client.config.override.policy=All`。适配器启动时检查
`read_committed` 与 `earliest` 配置；worker 必须实际应用这些覆盖值。
不要设置 `errors.tolerance=all` 或 `transforms`，适配器启动时也会拒绝；
跳过某条原始事件后继续推进 checkpoint 会破坏逐事件处理保证。

创建 topic 时使用 `cleanup.policy=delete`，不要启用 compaction，并使
保留期覆盖最长停机和重放窗口。生产多 broker topic 须设置适当副本数及
`min.insync.replicas`。先确认 MO checkpoint 的 `last_offset+1` 没有小于
该 topic 分区的 earliest retained offset。插件在 assignment 和每次
`put` 前都查询 broker topic ID、`cleanup.policy` 和 beginning offset，
因此需要 Kafka `DESCRIBE_CONFIGS` 权限；若启用 compaction、同名 topic 重建
或保留期越过 MO checkpoint，
会在业务 DML 前失败。Connect 在插件 `open` 前初始化新 group 的位置，
因此这里必须设 `auto.offset.reset=earliest` 供 worker 启动；插件仍以
MO checkpoint seek 为恢复权威。

向 Connect REST `POST /connectors` 提交以下配置，替换地址和凭据：

```json
{
  "name": "mo-eos-orders",
  "config": {
    "connector.class": "io.matrixone.connect.eos.AtomicSinkConnector",
    "tasks.max": "4",
    "topics": "mo_eos_events",
    "key.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
    "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
    "consumer.override.isolation.level": "read_committed",
    "consumer.override.auto.offset.reset": "earliest",
    "mo.jdbc.url": "jdbc:mysql://MO_HOST:6001/eos?connectTimeout=5000&socketTimeout=30000",
    "mo.jdbc.user": "SINK_USER",
    "mo.jdbc.password": "SINK_PASSWORD",
    "mo.scope": "orders",
    "mo.kafka.bootstrap.servers": "KAFKA_HOST:9092",
    "mo.batch.size": "64"
  }
}
```

若 Kafka 启用 TLS/SASL，把管理客户端相同的属性加为
`mo.kafka.client.security.protocol`、`mo.kafka.client.sasl.mechanism` 等；
不能用 `mo.kafka.client.bootstrap.servers` 覆盖管理客户端集群；
`mo.kafka.bootstrap.servers` 必须指向 worker consumer 使用的同一集群。
consumer 的安全属性仍由 worker/Connect 配置管理。限制数据库账号只能
读写本 profile 与业务表。不要在版本控制里保存真实凭据。

插件在 `open` 中锁住 checkpoint，增加 owner epoch，再将 Kafka 消费者
seek 到 MO `last_offset+1`。`put` 串行处理每个分区，完整事务提交后才
返回，`preCommit` 只提供 MO 已确认的位点。遇到 COMMIT 回执不确定，
插件换连接读账本和 checkpoint 判定；状态矛盾时停下等待接管或人工核对。
Kafka group committed offset 不是恢复权威。

## 3. NiFi 2.4 入口流程

在 NiFi 建立 `Kafka3ConnectionService`，指向同一个 Kafka 集群。源处理器
在首次发送前生成并固定上述完整 JSON；每个 FlowFile 恰好一个事件，
在重试关系中保留原始 `event_id`、delta 和 payload。把 FlowFile 连接
至 `PublishKafka`：

| 属性 | 值 |
| --- | --- |
| Kafka Connection Service | 前述 `Kafka3ConnectionService` |
| Topic Name | `mo_eos_events` |
| Delivery Guarantee | `Guarantee Replicated Delivery` (`acks=all`) |
| Failure Strategy | `Rollback`，或将 failure 接至保留原 FlowFile 的有界重试队列 |
| Message Demarcator | 留空，整个 FlowFile 作为一条消息 |
| Transactions Enabled | `true` 时 Connect 必须使用 `read_committed` |

不要在失败关系重新生成 event ID。NiFi `success` 后还需按事件 ID 对账
MO ledger 与 totals，并监控 Connect task 状态及 Kafka/MO 位点差距。
NiFi `PutSQL` 的 COMMIT OK 丢失重试不具有此账本恢复语义。

## 4. 恢复和运维

MO 事务前故障回滚账本、业务变更和位点；MO 提交后 Kafka offset 确认前
故障由 checkpoint seek 恢复。新 owner 的 epoch 会拒绝旧任务。Kafka
事务/control offset 可能跳号，插件按实际交付顺序推进，不要求数字连续。
相同 ID 不同摘要为数据冲突，需修正源事件或显式隔离；不可盲目 ACK。
账本默认永久保留。备份/恢复必须同时包含账本、预算、checkpoint 和
业务表的一致数据库恢复点；清理账本需要另行设计最大回放窗口。

这里的保障范围是 **MO 事务内 exactly-once business effects**，前提是
源事件 ID 稳定、Kafka 保留期足够、适配器按本协议运行。Kafka source
EOS、幂等快照 sink 和跨 NiFi/Kafka/MO 的完整分布式 EOS 属于不同等级。
设计和故障验收范围见
[`issue_29857_sink_profile.md`](../design/issue_29857_sink_profile.md)。
