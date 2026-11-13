# mo_index_stats 索引统计设计

## 背景与目标

持续的数据更新可能使 IVF 保存的中心与当前桶内分布失配，也会改变索引规模、桶负载和全文词典。DDL 能说明索引配置，实际分布和异步维护进度需要读取索引内容。

提供统一入口 `mo_index_stats(database, table, index)`，按索引类型返回可用于判断索引现状的指标。设计约束：

- 只读、按需计算，复用现有索引和引擎能力，不保存统计基线，也不修改索引维护路径。
- 固定外层返回结构，指标按类型组织，汇总与明细分开。
- 展示实际数量、归一化分布和明确的异常原因；不输出自动重建或整理建议。

当前支持主键、唯一索引、普通 BTREE 二级索引、IVF_FLAT、全文 v1/v2；空间索引、其他向量索引及订阅表不在支持范围内。

## 查询方式

```sql
SELECT * FROM mo_index_stats('数据库名', '表名', '索引名') AS s;

-- 格式化查看 IVF 汇总
SELECT JSON_PRETTY(metrics) AS stats
FROM mo_index_stats('ivf_demo', 'vectors', 'idx_embedding') AS s
WHERE row_type = 'index';
```

三个参数须为非空字符串字面量，调用者须有源表 SELECT 权限。目前仅支持本地普通表。主键名为 `PRIMARY`；其他索引未显式命名时，通过 `SHOW INDEX` 的 `Key_name` 获取生成名称：

```sql
SHOW INDEX FROM key_stats_demo.records;
```

未声明主键的表不支持查询 `PRIMARY`。MySQL 命令行可用 `\G` 替换分号，纵向展示。

## 输出契约

| 列 | SQL 类型 | 含义 |
| --- | --- | --- |
| `index_type` | VARCHAR | `primary`、`unique`、`secondary`、`ivfflat` 或 `fulltext`。 |
| `row_type` | VARCHAR | `index` 为汇总，`centroid` 为 IVF 桶明细。 |
| `object_id` | VARCHAR，可为 NULL | 桶编号字符串；汇总行为 SQL NULL。 |
| `metrics` | JSON | JSON 指标对象，保留数字、布尔值、数组等类型。 |

类型专属指标放在 `metrics` 中，新增类型无需修改外层列；JSON 数字可直接转换后参与 SQL 筛选和排序。

IVF 返回一行汇总及当前版本的全部桶，包含空桶；其他类型只返回一行汇总。配置、存储和异步进度只放在汇总行。无法计算的失配率和分母为零的倍数省略，有效的 `0` 保留；异步覆盖状态和延迟无法判断时为 JSON null。`status` 仅在异常或统计受限时出现。

## 主键、唯一索引和普通二级索引设计

| 指标 | 含义 |
| --- | --- |
| `columns` | 用户定义的键列，函数索引显示表达式，不含内部主键后缀。 |
| `method` | 索引方法，当前为 `btree`。 |
| `entry_count` | 可见索引条目数；主键统计源表行数，其他类型统计辅助表。 |
| `cardinality` | 不同逻辑键数量；复合键按完整元组去重，前缀索引按前缀去重，排除内部主键后缀。主键基数等于条目数。 |
| `prefix_lengths` | 前缀长度映射，例如 `{"name":2}`；有配置时返回。 |

主键执行源表 `COUNT(*)`，利用唯一性约束令基数等于条目数。唯一和普通二级索引先统计实际辅助表条目数，再按存储的主键引用关联源行，对逻辑键分组计数。关联条目数必须等于辅助表条目数，否则报错。

逻辑键排除存储专用的主键别名，保留用户显式声明的主键列；前缀使用 `SUBSTRING`，函数键读取已生成的列。对这些键表达式直接 `GROUP BY`，由 SQL 引擎按列的比较语义处理排序规则和 NULL。

已存储的 NULL 组合算一个键；部分唯一索引格式不存 NULL 条目，因此条目数可能少于源表行数。例如键 `1、1、2、NULL、NULL` 的条目数为 5，基数为 3。

基数按逻辑等价关系计数：在 `utf8mb4_unicode_ci` 下，`'Alpha'`、`'alpha'`、`'ALPHA '`、NULL 对应 2 个键，即使旧索引格式保存了不同的字节编码。

```sql
SELECT index_type,
       (metrics->>'$.entry_count')::BIGINT AS entry_count,
       (metrics->>'$.cardinality')::BIGINT AS cardinality
FROM mo_index_stats('key_stats_demo', 'records', 'category') AS s;
```

## IVF 统计设计

先读取 metadata 中唯一的当前版本，再读取该版本的全部中心，初始化包含空桶的统计状态。流式扫描同版本 entries，每条记录只更新所属桶的计数和几何累加量。存储规模统计另行覆盖该索引的全部辅助表及保留版本。

### 汇总指标

| 指标 | 含义 |
| --- | --- |
| `index_version` | 当前索引代次；重建后变化，普通更新不换代。异步未初始化时为 null。 |
| `lists` | 配置的桶数，可能与当前实际桶数不同。 |
| `op_type` | 距离类型；目前仅 `vector_l2_ops` 支持中心失配率，其他类型返回桶分布。 |
| `quantization` | 量化或类型转换方式：`none`、`float32`、`float16`、`bf16`、`int8`、`uint8`。 |
| `vector_count` | 当前版本的向量条目总数；异步索引可能尚未覆盖全部源数据。 |
| `centroid_count` | 实际桶数；未训练时可能只有 NULL 中心占位记录。 |
| `empty_centroid_count` | 空桶数。 |
| `empty_centroid_ratio` | 空桶数 / 实际桶数；没有桶时省略。 |
| `min_bucket_size` | 最小桶的向量数，包含空桶。 |
| `max_bucket_size` | 最大桶的向量数。 |
| `avg_bucket_size` | 总向量数 / 实际桶数；没有桶时为 0。 |
| `bucket_size_cv` | 桶大小的总体标准差 / 平均桶大小；越大越不均衡，平均值为 0 时返回 0。 |
| `max_bucket_size_multiple` | 最大桶 / 平均桶大小；例如 10 表示平均大小的 10 倍，平均值为 0 时省略。 |
| `center_mismatch_ratio` | 固定现有分桶、将中心移到桶均值后可消除的平方误差占比，范围 0～1。 |

### 桶明细指标

| 指标 | 含义 |
| --- | --- |
| `vector_count` | 当前桶的向量数。 |
| `bucket_size_multiple` | 当前桶 / 全部桶的平均大小；平均值为 0 时省略。 |
| `center_mismatch_ratio` | 当前桶的归一化中心失配率；空桶或无法完整计算时省略。 |

### 中心失配计算

设桶有 `n` 个向量 `x_i`，当前均值为 `m`，保存的中心为 `c`：

```text
桶失配率 = n × ||m-c||² / Σ||x_i-c||²
总体失配率 = 各桶分子之和 / 各桶分母之和
bucket_size_cv = sqrt(Σ(n_j-μ)² / K) / μ
```

其中 `K` 包含空桶，`μ` 为平均桶大小。逐桶在线更新向量相对保存中心的均值及平方距离和，避免保存全部向量；总体失配率按平方误差加权，不直接平均桶比值。

有向量且平方误差为 0 时，失配率为 0；无向量时省略。`0.65` 表示可消除约 65% 的平方误差，不表示分错桶的向量比例、召回率损失或重建收益。等比例缩放不改变此值，跨业务没有统一重建阈值。

同步量化索引使用源表原始向量计算失配率；异步量化索引省略失配率，异步非量化索引使用已维护的索引向量。

查看失配率最高的十个桶：

```sql
SELECT object_id AS centroid_id,
       (metrics->>'$.vector_count')::BIGINT AS vector_count,
       ROUND((metrics->>'$.bucket_size_multiple')::DOUBLE, 2) AS bucket_size_multiple,
       ROUND((metrics->>'$.center_mismatch_ratio')::DOUBLE * 100, 2) AS mismatch_pct
FROM mo_index_stats('ivf_demo', 'vectors', 'idx_embedding') AS s
WHERE row_type = 'centroid'
  AND metrics->'$.center_mismatch_ratio' IS NOT NULL
ORDER BY (metrics->>'$.center_mismatch_ratio')::DOUBLE DESC,
         (metrics->>'$.vector_count')::BIGINT DESC
LIMIT 10;
```

`mismatch_pct` 是失配率乘 100 后的查询别名。

### status

| 值 | 含义 |
| --- | --- |
| `index_not_initialized` | 异步索引尚未初始化当前版本。 |
| `untrained` | 存在 NULL 中心占位记录，尚未完成训练。 |
| `no_centroids` | 当前版本没有中心，仅汇总行返回。 |
| `incomplete_vectors` | 部分条目缺少可计算的向量。 |
| `center_stats_unsupported_distance` | 距离类型不支持失配率，目前包括 `vector_l2sq_ops`。 |
| `center_stats_unavailable_async_quantized` | 异步量化索引仅统计桶数量和分布。 |

## 全文统计设计

| 指标 | 含义 |
| --- | --- |
| `version` | 全文实现版本，1 或 2。 |
| `columns` | 覆盖的源表列，按声明顺序排列。 |
| `parser` | 分词器，例如 `ngram`、`gojieba`、`json`、`json_value`；默认值为 `ngram`。 |
| `document_count` | 索引中有效文档数。 |
| `term_count` | 有效词项的去重数量。 |
| `avg_document_frequency` | 所有有效词项的平均覆盖文档数。 |
| `min_document_frequency` | 最小覆盖文档数。 |
| `max_document_frequency` | 最大覆盖文档数。 |

同一词项在一篇文档出现多次，覆盖数仍算 1。例如三篇文档为 `apple apple banana`、`apple pear`、`banana`，则文档数为 3、词项数为 3，覆盖数为 2、2、1，平均值为 5/3，最小值为 1，最大值为 2。空词典的词项数和三个覆盖数指标均为 0。

**全文 v1**：通过 `__DocLen` 标记对文档 ID 去重得到文档数；排除该标记后按词项分组、对文档 ID 去重，再汇总覆盖数的平均值和最小/最大值。平均值以 DOUBLE 计算，避免整数 AVG 的固定小数位舍入。未产生词项的文档不会写入这种索引。

**全文 v2**：通过私有 `Fulltext2Search` 加载快照中的索引，调用 `Stats` 复用有效版本及删除判定。各 segment 的有序词典通过最小堆归并，同词项合并有效覆盖数，排除旧副本、已删文档及无效词项；不构建全量词典映射。沿用现有加载预算，不写入共享查询缓存，完成后 `Destroy` 释放映射。

两种实现均只返回全局汇总，结果规模不随词项数增加。

```sql
SELECT JSON_PRETTY(metrics) AS stats
FROM mo_index_stats('fulltext_demo', 'documents', 'ft_body') AS s;

SELECT JSON_PRETTY(metrics) AS stats
FROM mo_index_stats('fulltext_demo', 'documents_v2', 'ft_body_v2') AS s;
```

## 共享元数据设计

| 指标 | 含义 |
| --- | --- |
| `storage` | 下述存储规模，所有支持类型均返回。 |
| `index_options` | 其他已保存配置，值为字符串；不重复已单独输出的配置，无额外配置时省略。适用于键索引和 IVF。 |
| `maintenance_mode` | `sync` 为写入时同步维护，`async` 为后台维护；仅 IVF 和全文返回，全文 v2 固定为异步。 |
| `async` | 异步维护进度，仅异步索引返回。 |

IVF 的附加配置可能包括：`kmeans_train_percent`（训练数据百分比）、`kmeans_max_iteration`（训练迭代上限）、`auto_update`（自动维护）、`day` / `hour` / `second`（维护间隔）、`included_columns`（INCLUDE 列的 JSON 编码字符串）、`max_bad_records`（允许跳过的异常记录上限）。这些是建索引配置，不是本次统计的测量值。

### storage

| 字段 | 含义 |
| --- | --- |
| `size_bytes` | 引擎估算的列压缩前数据体积，单位字节，包含已发布统计和内存/事务工作区数据。 |
| `table_count` | 承载索引的不同物理表数量。 |
| `object_count` | 可见持久化数据对象和删除对象总数，同一对象按多列去重。 |

辅助表统计包含仍保留的旧版本和异常记录表，不含源表。主键共享源表：大小只统计声明的主键列，不计其他列或额外复合键序列化列；表数为 1，对象数为共享源表对象数。

`size_bytes` 累加各辅助表的 `Relation.Size("*")`；主键改为逐个声明列调用。对象元数据分别读取数据和删除对象，按对象名去重后求和。

`Relation.Size` 优先复用引擎已发布的统计；冷缓存或统计与表版本不匹配时，可能同步构建统计并等待，带来额外计算和 I/O。诊断不主动刷盘，也不保存诊断基线。

大小估算不等于压缩后的磁盘文件大小或进程内存占用；全文 v2 的列内二进制本身可能已压缩。数据未落盘时，对象数可为 0。

### async

| 字段 | 含义 |
| --- | --- |
| `task_state` | `running` 正常运行或等待，`retrying` 记录错误后重试，`stopped` 永久错误停止，`canceled` 已取消；无任务时省略。 |
| `synced_at` | 持久化消费水位对应的 UTC 时间；无有效水位时省略。 |
| `caught_up` | `true` 已证明覆盖源表所需水位，`false` 尚未证明覆盖，null 无法判断。 |
| `lag_seconds` | 源表所需水位与消费水位的非负物理时间差；无法计算时为 null。 |
| `error_code` | 非零维护错误码，无错误时省略。 |
| `error_message` | 非空维护错误描述。 |
| `status` | 进度不可用或初始化中的原因，正常时省略，取值见下表。 |

| `async.status` | 含义 |
| --- | --- |
| `task_missing` | 没有有效维护任务。 |
| `initializing` | 初始化中，`caught_up=false`、`lag_seconds=null`。 |
| `checkpoint_unavailable` | 无有效持久化消费水位。 |
| `job_status_unavailable` | 缺少有效任务阶段。 |
| `source_commit_ts_unavailable` | 无法取得源表所需水位，例如事务内有未提交写入。 |

从 `mo_catalog.mo_iscp_log` 读取该源表最新、未删除的 `index_<索引名>` 任务。解析持久化水位后，向源表的 `SourceCommitTS` 获取当前快照所需的覆盖水位：

```text
caught_up = 消费水位 >= 源表所需水位     // 比较完整物理和逻辑时间
lag_seconds = max(0, 源表物理时间 - 消费物理时间) / 1秒
```

源表所需水位包含存储保留边界，消费水位以持久化记录为准，拒绝最大时间戳等哨兵值。`running` 不代表追平，`false` 不表示已测得积压条数；只有逻辑时间落后时可出现 `caught_up=false`、`lag_seconds=0`。延迟不按当前时间计算，源表无更新时旧水位也可已追平。

## 异常处理与计算开销

参数错误、目标不存在、不支持的类型、权限不足以及底层读取失败返回 SQL 错误。缺失中心引用、重复中心、向量维度不符、非有限数值、计算溢出、键关联数量不符等数据异常同样报错；未训练、暂不支持几何计算、无法确定异步水位等可解释状态，通过上述 `status` 返回可用的部分指标。

| 类型 | 主要成本 |
| --- | --- |
| 主键 | 精确统计源表可见行数。 |
| 唯一和普通二级索引 | 辅助表计数、关联源表和逻辑键分组，资源由 SQL 执行器管理。 |
| IVF | 对 `N` 个 `d` 维向量做约 O(Nd) 几何计算，保留 O(Kd) 桶状态；不计算几何时只统计桶计数。 |
| 全文 v1 | 倒排数据分组和去重，资源由 SQL 执行器管理。 |
| 全文 v2 | 加载索引并遍历词典及有效 posting；词典归并额外内存为 O(segment 数)，不包含索引加载内存。 |

当前不采样，不缓存诊断结果。输出过滤和 `LIMIT` 在统计完成后生效，不减少完整统计开销。流式批次消费后立即释放，错误和取消向上传递，全文 v2 在退出时释放私有读取器。
