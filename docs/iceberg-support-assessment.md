# MatrixOne 增加 Apache Iceberg 支持的调研与工作量评估

> 调研基于 MatrixOne 当前源码（分支 main，commit `8ad7a3f` 时点）+ 公开资料检索。
> 评估目标：①把 Snowflake / Databricks 的 Iceberg 表作为外部数据源读取；②把 MatrixOne 数据架在 Iceberg 上、让 Snowflake 接入；③评估"绕开 Iceberg、用纯 Parquet 互通"的可行性；④对标 Doris / StarRocks / TiDB / ClickHouse 等产品的 Iceberg 支持现状。

---

## 0. 结论速览（TL;DR）

| 工作项 | 推荐路线 | 工作量（生产级） | 可行性 |
|---|---|---|---|
| **工作1**：MO 读 Snowflake/Databricks 的 Iceberg | 外部 Catalog（REST/HMS）+ 复用现有 Parquet reader + fileservice | **MVP 1.5–3 人月 / 生产 3–8 人月** | ✅ 高，地基齐全 |
| **工作2b**：MO 发布 Iceberg 供 Snowflake 读（旁路镜像层） | 保留 TAE，用 CDC 物化出 Parquet+Iceberg 副本 + 跑 Iceberg REST Catalog 服务端 | **6–10 人月** | ✅ 推荐 |
| 工作2a：原生 Iceberg 存储（替换 TAE 自研格式） | 重写存储引擎持久化层 | **12+ 人月，高风险** | ❌ 不建议 |
| **纯 Parquet 互通**（不走 Iceberg） | 复用现有外部表读 + `SELECT INTO OUTFILE FORMAT=parquet` 导出 | **读 0（已支持）/ 写 0–1 人月** | ✅ 已基本可用，但只有"批量交换"语义 |

**核心判断**：
- **"读 Iceberg"性价比高、地基齐**——MatrixOne 已经会读 Parquet、访问对象存储、且有可插拔外部表框架，只缺 Iceberg 元数据解析这一层。
- **"对外发布给 Snowflake"该走旁路 CDC 发布层**，不要去重写 TAE 存储引擎。
- **纯 Parquet 能做"数据交换/批量 ETL"级互通，今天就基本能跑**；但拿不到事务/快照/增量/行级更新删除等"活表"语义——要"活表"就绕不开 Iceberg。
- **行业背景**：Iceberg 已是事实标准开放表格式，Doris/StarRocks/ClickHouse/Snowflake/BigQuery/Redshift/Trino/Databricks 等几乎全员支持；**MatrixOne 是这张表里少数的空白**。

---

## 1. MatrixOne 现状盘点：哪些"地基"已经有

评估工作量的关键是看现有代码能复用多少。结论：**读 Iceberg 的地基相当完整，写/对外发布 Iceberg 的地基几乎为零**。

| 能力 | 现状 | 关键代码 |
|---|---|---|
| 对象存储接入（S3/OSS/MinIO/HDFS/本地） | ✅ 成熟 | `pkg/fileservice/`（~29K 行），`ObjectStorage` 接口 `object_storage.go`；已依赖 aws-sdk-go-v2、aliyun-oss、minio-go |
| **Parquet 读取**（Iceberg 的数据文件格式） | ✅ 成熟 | `pkg/sql/colexec/external/parquet*.go`，依赖 `parquet-go v0.25.1`；支持 row-group 裁剪、S3 预取、嵌套类型、大小写折叠 |
| **Parquet 导出** | ✅ 已支持 | `SELECT ... INTO OUTFILE FORMAT='parquet'`，`pkg/frontend/export_parquet.go`（`NewParquetWriter`）+ `export.go:932`；导出走 `fileservice.GetForETL`，**可直接写 S3/OSS/Stage** |
| 外部表框架（可插拔 reader） | ✅ 成熟 | `ExternalFileReader` 接口 `external/types.go:109`；格式靠 `Format` 字段分发 `external.go:152` |
| 谓词下推 / zonemap 过滤 / 分区裁剪 | ✅ 有（自有格式 + Hive 分区） | `FilterParam` `types.go:97`；`hive_partition.go`（树状谓词求值、分区发现与缓存） |
| STAGE（命名 + 带凭证的外部存储位置） | ✅ 有 | `tree/stages.go`、`table_function/stage.go`；可作为挂 Iceberg catalog 凭证的天然落点 |
| CDC / 变更捕获（可驱动增量写） | ✅ 有 | `pkg/cdc/`（~32K 行）、`Relation.CollectChanges`；含多种 sinker |
| 快照 / PITR / 时间旅行概念 | ✅ 有 | `pkg/frontend/snapshot.go`、`pitr.go` |
| **Iceberg 表元数据层** | ❌ 完全没有 | 全仓库 `grep iceberg` = 0 命中 |
| **Avro 编解码**（Iceberg manifest 用 Avro） | ❌ 无库 | go.mod 无 avro 依赖 |
| **外部 Catalog 抽象层**（HMS / Iceberg REST / Glue / Unity） | ❌ 完全没有 | 外部表是只读数据源，元数据以 JSON 序列化进 MO 内部 catalog（`SystemExternalRel`） |
| **底层数据格式** | ⚠️ 完全自研，非 Parquet | `pkg/objectio/`（~15K 行）自研列存二进制；TAE 引擎 `pkg/vm/engine/tae/`（~134K 行）围绕它构建 |

**一句话**：MatrixOne 已经会"读 Parquet + 写 Parquet + 访问对象存储 + 插拔式外部表"，但它自己的数据**不是** Parquet，而是 TAE 引擎的自研格式（objectio：自定义 block/column/zonemap/bloomfilter 布局，163B ObjectStats、64B BlockHeader、LZ4 压缩）。

### 1.1 MatrixOne 存储引擎要点（与"写 Iceberg"强相关）

- **存储引擎 TAE**（`pkg/vm/engine/tae/`，~134K 行）：catalog（元数据/MVCC）、tables（append/normal object）、logtail（checkpoint）、logstore（WAL）、txn（MVCC）。
- **自研列存格式 objectio**（`pkg/objectio/`，~15K 行）：层级为 **Object → Block → Column**；Block 最大 8192 行；列级 LZ4 压缩；每列带 ZoneMap(min/max) 与 BloomFilter。
- **写入路径**：INSERT → 内存 append object（aobj）→ flush（`tables/jobs/flushobj.go` 用 `BlockWriter` 写 objectio 文件）→ FlushTableTail/compaction 合并 → 落 FileService。
- **MVCC**：基于 `TxnMVCCNode` 版本链（Create/Delete 链）+ 时间戳隔离；checkpoint 保存目录快照。
- 这套体系与 objectio 自研格式深度耦合，是"原生替换为 Iceberg（2a）"风险极高的根因。

---

## 2. 工作1：把 Snowflake / Databricks 的 Iceberg 表作为外部源读取

**本质**：MatrixOne 当一个 **Iceberg Reader**。两件事里 ROI 最高、最可控。

### 2.1 要做的事
1. **Iceberg 元数据层**（核心难点）：解析 `metadata.json`（schema / partition-spec / 快照列表）→ manifest-list（Avro）→ manifest（Avro）→ 定位一批 Parquet 数据文件；处理快照选择、schema/partition 演进、sequence number。
2. **Avro 解码器**：manifest 是 Avro 格式，需引入 `hamba/avro` 之类的库。
3. **REST Catalog 客户端**：Snowflake（Open Catalog / Polaris）和 Databricks（Unity Catalog）**都对外暴露 Iceberg REST Catalog 接口**，AWS Glue 也兼容 → **一个 REST 客户端基本通吃**。难点在鉴权（OAuth、vended-credentials 凭证派发）。
4. **接到现有外部表框架**：新增 `IcebergReader` 实现 `ExternalFileReader` 接口；新增 `format='iceberg'` 或 `CREATE EXTERNAL TABLE ... CATALOG` 语法（`build_ddl.go:923` 的 option 白名单加几项）；数据文件读取直接复用现成 Parquet reader。
5. **Iceberg v2 delete 文件**（position/equality delete 的 merge-on-read）+ 类型映射 + 分区裁剪下推。

### 2.2 重大加速因素
Apache 官方有 **`github.com/apache/iceberg-go`**（纯 Go 的 Iceberg 库，含 REST catalog、metadata、manifest 解析）。可直接 vendor/适配，省掉第 1、2、3 项的大部分自研，把工作1 主要变成"胶水 + 复用现成 Parquet reader"。

### 2.3 工作量
| 范围 | 估算 |
|---|---|
| MVP：REST catalog + 读快照 + 复用 Parquet reader + 基础分区裁剪（不含 delete 文件 / schema 演进），借助 iceberg-go | **~1.5 – 3 人月** |
| 生产级：v2 delete 文件、schema/partition 演进、谓词下推到 manifest、Snowflake+Databricks+Glue 多 catalog、凭证派发、全类型覆盖、容错与缓存 | **~3 – 8 人月** |

> 独立交叉验证：探索 agent 给出"生产级 Iceberg 读 ≈ 12–16 周（≈3–4 人月）"，拆为 元数据抽象层(4–6w) → reader 复用 Parquet(3–4w) → 接入 External 算子(2–3w) → manifest 裁剪/delete 优化(2–3w)，与上表一致。

---

## 3. 工作2：MatrixOne 把数据架在 Iceberg + Snowflake 接入

**本质**：MatrixOne 当一个 **Iceberg Writer / Catalog**，让外部引擎（Snowflake）读到 MO 数据。比工作1 大一个量级，因为 MO 原生存储不是 Parquet/Iceberg。两条路线：

### 3.1 路线 2a：原生 Iceberg 存储（用 Parquet+Iceberg 替换 objectio）
- 需重写：flush、compaction/merge、读路径、checkpoint、logtail、GC，把自研 zonemap/bloom/MVCC 映射到 Iceberg 的统计/快照/delete 文件。
- TAE 是 ~134K 行、与 objectio 深度耦合的核心引擎，等于**重做一个存储引擎**。
- **风险极高，~12+ 人月（实际跨季度多人项目）。不推荐。**

### 3.2 路线 2b：Iceberg "发布/镜像"层（保留 TAE，旁路导出 Iceberg + 跑 REST Catalog 服务）⭐ 推荐
TAE 内部照旧，新增一个后台服务：消费 MO 已提交变更（**复用现有 CDC / `CollectChanges` / logtail**），在对象存储上物化出一份并行的 Iceberg 副本——
- 写 Parquet（`parquet-go` 已支持写，需产品化）；
- 写 Avro manifest（需引入 Avro 库）；
- 生成并**原子提交** `metadata.json` / snapshot；
- 起一个**符合 Iceberg REST Catalog 规范的服务端** + 给数据文件签发访问凭证，让 **Snowflake 用"外部 Iceberg 表 / catalog-linked database"挂载读取**；
- MO 的删除映射成 Iceberg delete 文件，或走 copy-on-write。

代价：数据双写（额外存储成本）、最终一致性（同步延迟）。

### 3.3 工作量
| 路线 | 估算 |
|---|---|
| 2a 原生替换存储引擎 | **~12+ 人月，高风险，不推荐** |
| 2b 发布/镜像层 + REST Catalog 服务端（生产级） | **~6 – 10 人月** |

> 工作1 做出的"Iceberg 元数据**读**"与工作2b 需要的"元数据**写** + catalog 服务"互补；先做工作1 能沉淀出可复用的 Iceberg core 库（类型系统、manifest 模型、Avro 编解码），降低工作2 成本。

---

## 4. 替代方案：不走 Iceberg，用纯 Parquet 互通

**结论：可行，且两个方向今天基本都已能跑，工作量比 Iceberg 小一个数量级；代价是失去"活表"语义。**

### 4.1 现状（两个方向今天基本都通）
- **读方向**（MO 读 Snowflake/Databricks）：MO 已能 `CREATE EXTERNAL TABLE ... FORMAT='parquet'` 读对象存储 Parquet；Snowflake `COPY INTO @stage ... FILE_FORMAT=(TYPE=PARQUET)`、Databricks `df.write.parquet()` 卸载成 Parquet → **今天就能查，工程量≈0，剩下是运维编排（谁定期 unload + 刷新）**。
- **写方向**（Snowflake 读 MO）：MO 已有 `SELECT ... INTO OUTFILE FORMAT='parquet'`，且写 fileservice → 能直接写 S3/Stage；Snowflake 用外部表/`COPY INTO` 读 → **今天也基本能跑**。可能补：导出类型覆盖（`vectorValueToParquet` 有 unsupported type 分支）、文件命名约定、定时导出调度（**写方向顶多 0–1 人月**）。

### 4.2 纯 Parquet 相比 Iceberg 失去了什么（决策核心）
Parquet 是**文件格式**，Iceberg 是**表格式**（在一堆 Parquet 上加的元数据层）。

| 能力 | 纯 Parquet | Iceberg |
|---|---|---|
| 原子提交 / 一致性快照 | ❌ 读到一半在写 → 读到不一致 | ✅ snapshot 原子切换 |
| 增量同步 / CDC | ❌ 基本只能全量 dump | ✅ 增量 append + sequence |
| 行级 更新 / 删除 | ❌ Parquet 不可变，删一行要重写整文件 | ✅ delete file / 读时合并 |
| Schema 演进 | ❌ 两边手工协调 | ✅ 内建 |
| 文件裁剪 / 统计目录 | ⚠️ 只能靠目录约定(Hive分区)+单文件 footer，要 LIST 全目录 | ✅ manifest 带统计，跳文件 |
| Time travel / 版本 | ❌ | ✅ |
| 并发写冲突检测 | ❌ | ✅ 乐观并发 |

### 4.3 关键约束：互通"标准"由 Snowflake 认什么决定
**Snowflake 端只认两种东西——裸 Parquet 外部表（无事务语义）和标准 Iceberg，中间没有第三种它认的格式。** 因此即便在 Parquet 上自建"轻量清单/版本指针"，也只能优化 **MO 自己读自己**；要让 Snowflake 也享受一致性快照/增量，就必然回到标准 Iceberg。

### 4.4 怎么选
- 需求 = **批量数据交换**（能容忍延迟、全量刷新、只读、无 upsert/delete）→ **纯 Parquet 足够，今天就能上**。
- 需求 = **共享同一张活表**（近实时、ACID、可更新删除、time travel）→ **绕不开 Iceberg**。

---

## 5. 同类产品的 Iceberg 支持现状（对标）

### 5.1 Apache Doris ✅ 读写都全
| 阶段 | 版本 / 时间 | 能力 |
|---|---|---|
| 最早试水 | 0.15（约 2021 底） | 老式 Iceberg 外部表（`ENGINE=ICEBERG`） |
| 成熟的读 | **1.2（2022 底/2023 初）** | Multi-Catalog，Iceberg Catalog 支持 HMS + REST |
| 读写增强 | 2.1（2024.04） | Iceberg/Hudi/Paimon 读写 |
| 完整 DML/DDL | 2.1.6（2024） | 直接 `CREATE DB/TABLE` + `INSERT/UPDATE/DELETE/MERGE INTO` |

### 5.2 StarRocks ✅ 与 Doris 同期，读写齐全
| 阶段 | 版本 / 时间 | 能力 |
|---|---|---|
| 读（Iceberg Catalog） | **2.4（2022.12）** | 引入 Hudi+Iceberg external catalog，直接查询免建外部表 |
| 写（Sink） | **3.1（2023）** | `CREATE DB/TABLE`、`CTAS`、`INSERT INTO/OVERWRITE` 写 Parquet 格式 Iceberg |
| 持续增强 | 3.2–4.1（2023–2025） | 元数据缓存(HMS/Glue/Tabular)、sink 优化、2025 与 Apache Polaris 集成 |

### 5.3 ClickHouse ✅ 近两年猛追，已读写对等
| 版本 / 时间 | 能力 |
|---|---|
| 23.2（2023.02） | 首次直接查询 Iceberg（表引擎/表函数），**只读** |
| 24.12（2024.12） | **REST Catalog**（Polaris、Unity）；读 position/equality **delete**（MoR） |
| 25.3（2025.03） | AWS Glue、Databricks Unity；`DataLakeCatalog` 引擎 |
| 25.4（2025.04） | **Time travel**（查历史快照） |
| 25.5（2025.05） | Hive Metastore |
| 25.8（2025.08） | **写入** `INSERT` + delete 语义 |
| 25.10（2025.10） | `ALTER UPDATE`、分布式写 → **读写对等** |
| 25.11（2025.11） | Microsoft OneLake |

> ClickHouse 同样是自研列存（MergeTree，底层非 Parquet），但它**没有改存储引擎**，而是走"外部 Catalog + Parquet 读 + 旁路 sink"——正是本报告给 MO 推荐的路线，已被验证可行。

### 5.4 TiDB ⚠️ 无原生查询，走 CDC 下游
- TiDB 是 OLTP/HTAP 事务库（TiKV 行存 + TiFlash 列存），**没有"把 Iceberg 当外部表查"的能力**。
- Iceberg/湖仓关系是 **TiCDC 把变更同步到对象存储（S3/GCS/Azure）/ Microsoft OneLake（Open Mirroring → Iceberg/Delta）**——TiDB 喂数据给 Iceberg 湖仓，而不是自己查。
- TiDB X（2025.10）把对象存储做成存储底座，是其**自身**架构，非 Iceberg 互操作。

### 5.5 更广格局：基本"全员支持"
Iceberg 被 30+ 引擎共享读写：

| 类别 | 产品 | Iceberg 支持 |
|---|---|---|
| 云数仓 | Snowflake | 原生托管 + 外部表，Polaris 开放 Catalog |
| | Google BigQuery | BigLake 托管 Iceberg，读写 |
| | AWS Redshift | 2025 GA 写入（CREATE TABLE/INSERT，S3 Tables） |
| | Databricks | 2024/2025 完整 Iceberg 支持，Unity Catalog |
| 开源查询引擎 | Trino / Presto | connector 最成熟，读写、联邦分析标杆 |
| | Dremio | 强读 + Nessie/Arctic catalog |
| | DuckDB | iceberg 扩展，读（演进中） |
| | Spark / Flink / Athena / EMR / Hive / Impala | 均支持 |
| OLAP（MO 同类） | StarRocks / Doris / ClickHouse | 读写均已支持（见上） |
| | TiDB | 仅 CDC 下游 sink |
| | **MatrixOne** | ❌ 目前 0 |
| 托管湖/底座 | AWS S3 Tables、Microsoft Fabric/OneLake、Confluent Tableflow | 以 Iceberg 为存储格式 |

---

## 6. 总体建议

1. **优先做工作1（读 Iceberg）**：ROI 最高，MO 已有 Parquet reader + 对象存储 + 外部表框架，复用度极高；优先评估直接集成 `apache/iceberg-go`。**学 Doris/StarRocks/ClickHouse 的"外部 Catalog + 复用 Parquet"路线。**
2. **工作2 走 2b（CDC 旁路发布层），不要碰 2a（重写引擎）**：让 MO 成为合规 Iceberg REST Catalog + 在 S3 发布 Parquet+Iceberg 元数据，Snowflake 以外部 Iceberg 表接入。**学 TiDB 的 CDC 下游 sink 思路，复用 MO 现有 CDC 模块。**
3. **若只需批量交换、不需活表语义**：直接走**纯 Parquet 互通**——基本是配置/调度工作，今天就能打通，零到 1 人月。
4. **行业判断**：Iceberg 已是事实标准，MO 三个最近竞品（Doris/StarRocks/ClickHouse）全部支持且已从读走到写；补齐 Iceberg 读支持是低风险的"补课"，有大量成熟先例可循。

---

## 7. 关键代码索引（便于后续落地）

| 模块 | 路径 |
|---|---|
| 外部表执行算子 / reader 接口 | `pkg/sql/colexec/external/external.go`、`types.go`（`ExternalFileReader`） |
| Parquet 读 | `pkg/sql/colexec/external/parquet.go`、`reader_parquet.go`、`parquet_nested.go` |
| Parquet 写（导出） | `pkg/frontend/export_parquet.go`、`export.go` |
| 外部表 DDL / option 白名单 | `pkg/sql/plan/build_ddl.go`（~L900–960） |
| 外部表参数 AST | `pkg/sql/parsers/tree/update.go`（`ExternParam` L215+，`S3Parameter`） |
| Hive 分区裁剪 | `pkg/sql/colexec/external/hive_partition.go`、`hive_partition_fill.go` |
| STAGE | `pkg/sql/parsers/tree/stages.go`、`pkg/sql/colexec/table_function/stage.go` |
| 对象存储抽象 | `pkg/fileservice/object_storage.go`、`s3_fs.go`、`aliyun_sdk.go`、`hdfs.go`、`minio_sdk.go` |
| 存储引擎 TAE | `pkg/vm/engine/tae/`（catalog/、tables/、logtail/、logstore/、txn/） |
| 自研列存格式 | `pkg/objectio/`（writer.go、meta.go、block.go、column.go、object_stats.go） |
| 写入/flush 路径 | `pkg/vm/engine/tae/tables/jobs/flushobj.go`、`flushTableTail.go`、`mergeobjects.go` |
| CDC（旁路发布可复用） | `pkg/cdc/`（sinker*.go）、`Relation.CollectChanges`（`pkg/vm/engine/types.go`） |

---

## 8. 参考来源

- ClickHouse Iceberg 表引擎文档 — https://clickhouse.com/docs/engines/table-engines/integrations/iceberg
- Climbing the Iceberg with ClickHouse — https://clickhouse.com/blog/climbing-the-iceberg-with-clickhouse
- The DataLakeCatalog engine in ClickHouse Cloud — https://clickhouse.com/blog/query-your-catalog-clickhouse-cloud
- StarRocks 2.4 发布公告（2022.12） — https://www.starrocks.io/blog/announcing-starrocks-version-2.4
- StarRocks Iceberg Catalog 文档 — https://docs.starrocks.io/docs/data_source/catalog/iceberg/iceberg_catalog/
- StarRocks 数据湖分析特性支持矩阵 — https://docs.starrocks.io/docs/data_source/feature-support-data-lake-analytics/
- Apache Doris Iceberg Catalog 文档 — https://doris.apache.org/docs/dev/lakehouse/catalogs/iceberg-catalog/
- Apache Doris × Iceberg 最佳实践 — https://doris.apache.org/docs/3.x/lakehouse/best-practices/doris-iceberg/
- TiCDC 概述 — https://docs.pingcap.com/tidb/stable/ticdc-overview/
- Microsoft Fabric × TiDB（OneLake Open Mirroring） — https://blog.fabric.microsoft.com/en-us/blog/elevating-microsoft-fabric-with-new-isv-solutions/
- Top 10 Query Engines for Apache Iceberg（对比） — https://estuary.dev/blog/comparison-query-engines-for-apache-iceberg/
- Announcing full Apache Iceberg support in Databricks — https://www.databricks.com/blog/announcing-full-apache-iceberg-support-databricks
- Apache Iceberg Go 库 — https://github.com/apache/iceberg-go
