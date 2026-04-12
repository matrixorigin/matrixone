# MatrixOne 当前全文索引实现拆解

## 1. 总体判断

**一句话总结：当前 MO 的全文检索实现，本质上是把原表文本切词后写进一张隐藏索引表，然后把 `MATCH ... AGAINST` 重写成对隐藏索引表的 SQL 查询，再在 Go 里做聚合和 TF-IDF / BM25 打分。**

它具备完整的“从 SQL 到结果”的功能链路，但离生产级全文检索还有明显距离，主要原因有四类：

1. **索引结构太原始**：只有 `doc_id / pos / word` 三列，没有 segment、posting list、跳表、预聚合词典、delete bitmap 等成熟搜索结构。
2. **查询阶段做了太多动态工作**：每次查询都要动态生成 SQL、拉取候选、在内存中聚合、再算分。
3. **写入维护是同步的**：建索引回填、INSERT / UPDATE / DELETE 维护都直接走 SQL 和切词，没有后台异步 refresh / merge。
4. **语义和分析器能力不成熟**：`natural language mode` 语义偏离主流，分词器非常简单，缺少 stopword、stemming、synonym、field norm 等能力。

---

## 2. 用户视角能用什么

### 2.1 支持的 SQL 入口

从 parser/binder/planner 来看，当前支持：

- `CREATE FULLTEXT INDEX ...`
- `ALTER TABLE ... ADD FULLTEXT INDEX ...`
- `CREATE TABLE ... FULLTEXT(...)`
- `MATCH(col1, col2, ...) AGAINST ('pattern' [mode])`

对应代码入口：

- 语法：`pkg/sql/parsers/dialect/mysql/mysql_sql.y`
- AST：`pkg/sql/parsers/tree/expr.go`
- binder 改写：`pkg/sql/plan/base_binder.go`

### 2.2 公开的搜索模式

parser 层面能识别：

- 默认模式
- `IN NATURAL LANGUAGE MODE`
- `IN BOOLEAN MODE`
- `WITH QUERY EXPANSION`
- `IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION`

但**真正执行层面只实现了默认 / natural language / boolean**，`query expansion` 在 `pkg/fulltext/sql.go` / `pkg/fulltext/fulltext.go` 里明确返回 unsupported。

### 2.3 支持的 parser

DDL 校验允许以下 parser：

- `default`
- `ngram`
- `json`
- `json_value`

对应代码：`pkg/sql/plan/build_ddl.go`

但需要注意：

- **`default` / `ngram` 实际走的是同一套 `SimpleTokenizer`**
- **`NgramTokenSize` 被固定写成 3，但实际切词路径并没有把它做成可配置分析器**

也就是说，当前的 “ngram parser” 更像是一个名字，而不是完整可配置的 analyzer。

---

## 3. 整体执行架构

### 3.1 逻辑图

```text
CREATE/ALTER FULLTEXT INDEX
  -> buildFullTextIndexTable()
  -> 创建隐藏索引表 (__mo_index_secondary_xxx)
  -> INSERT INTO idx
       SELECT f.*
       FROM src
       CROSS APPLY fulltext_index_tokenize(...)

INSERT/UPDATE/DELETE
  -> DELETE FROM idx WHERE doc_id IN (...)
  -> INSERT INTO idx
       SELECT f.*
       FROM src
       CROSS APPLY fulltext_index_tokenize(...)

SELECT ... WHERE MATCH(...) AGAINST(...)
  -> binder 生成 fulltext_match(pattern, mode, cols...)
  -> optimizer 找到匹配的 FULLTEXT index
  -> 改写成 fulltext_index_scan(...)
  -> fulltext_index_scan 生成 SQL 读取隐藏索引表
  -> Go 内存聚合并计算 TF-IDF/BM25
  -> 用 doc_id 回表 JOIN 原表
  -> score DESC 排序
```

### 3.2 当前隐藏索引表长什么样

隐藏索引表由 `buildFullTextIndexTable()` 构造，核心列是：

- `doc_id`: 原表主键类型
- `pos`: 词位置
- `word`: 分词结果
- 隐藏自增 fake pk

并且：

- **按 `word` cluster**
- 被标记为系统 index relation

核心代码：

- `pkg/sql/plan/build_ddl.go`
- `pkg/catalog/types.go`

### 3.3 当前实现更像什么

它更像一个**“行式 token 表”**，不是成熟搜索系统里的**“压缩 posting list / 段式倒排索引”**。

这点非常关键，因为后面所有性能与体验问题，几乎都可以追溯到这个基础结构选择。

---

## 4. 建索引链路

### 4.1 DDL 侧

`CREATE FULLTEXT INDEX` / `ALTER TABLE ADD FULLTEXT INDEX` 进入 planner 后，会：

1. 校验索引列类型  
   只允许：
   - `char`
   - `varchar`
   - `text`
   - `json`
   - `datalink`

2. 校验 parser  
   只允许：
   - `ngram`
   - `default`
   - `json`
   - `json_value`

3. 生成 `plan.IndexDef`
4. 生成隐藏索引表定义

代码位置：

- `pkg/sql/plan/build_ddl.go`

### 4.2 回填方式

compile 阶段真正创建索引时，`handleFullTextIndexTable()` 会：

1. 先建隐藏表
2. 再执行一条整表回填 SQL

回填 SQL 形态：

```sql
INSERT INTO `db`.`index_table`
SELECT f.*
FROM `db`.`src` AS src
CROSS APPLY fulltext_index_tokenize('params', src.pk, src.col1, src.col2, ...) AS f;
```

代码位置：

- `pkg/sql/compile/ddl_index_algo.go`
- `pkg/sql/compile/util.go`

### 4.3 这意味着什么

这说明当前建全文索引：

- **是同步回填**
- **回填时要重新扫描原表并重新切词**
- **没有“先建元数据，再后台 build，再增量 catch-up”的在线建索引机制**

对大表来说，这会直接带来：

- 建索引耗时长
- DDL 阻塞感强
- 资源峰值明显

---

## 5. 增量维护链路

### 5.1 INSERT

INSERT 时，planner 会为全文索引额外拼出：

```sql
INSERT INTO index_table
SELECT f.*
FROM source_rows
CROSS APPLY fulltext_index_tokenize(params, pk, col1, col2, ...)
```

代码位置：

- `pkg/sql/plan/build_dml_util.go`

### 5.2 DELETE

DELETE 时不会按词级增量删除，而是直接：

```sql
DELETE FROM index_table WHERE doc_id IN (...)
```

代码位置：

- `pkg/sql/plan/build_dml_util.go`
- `pkg/sql/colexec/postdml/postdml.go`

### 5.3 UPDATE

UPDATE 本质上就是：

1. 删除旧 `doc_id` 的全部 token
2. 重新读取新值，再切词插回去

也就是典型的：

**delete old + re-tokenize + insert new**

### 5.4 当前写路径的代价

这条链路对写入很不友好：

- 更新一个文本字段，要删整篇文档的所有 token，再重建全部 token
- 没有增量 posting merge
- 没有 delete bitmap / tombstone segment
- 没有异步 refresh
- 没有后台 compaction

所以文本字段一旦频繁更新，全文索引维护成本会明显放大。

---

## 6. 分词 / 分析器实现

### 6.1 default / ngram 实际行为

`fulltext_index_tokenize` 在 `default` / `ngram` / 空 parser 下都会调用：

- `tokenizer.NewSimpleTokenizer`

代码位置：

- `pkg/sql/colexec/table_function/fulltext_tokenize.go`
- `pkg/monlp/tokenizer/simple.go`

### 6.2 `SimpleTokenizer` 的真实规则

当前 tokenizer 大致规则如下：

1. **Latin/数字**
   - 按连续字母数字切 token
   - 统一 lower-case
   - 单个 token 最长 **23 bytes**

2. **CJK/非 Latin 连续文本**
   - 使用**3 rune 滑窗**
   - 并且在尾部继续输出 2 rune / 1 rune token

3. **标点和空白**
   - 作为 breaker

4. **没有这些能力**
   - stopword
   - stemming
   - synonym
   - 语言词典
   - 词性/词法分析
   - 字段权重
   - analyzer chain

### 6.3 `ngram` 为什么名不副实

DDL 里虽然会把 `NgramTokenSize` 写成 3，但查询/建索引实际并没有根据这个值切换不同 ngram 实现。  
所以当前更准确的描述应该是：

- **MO 现在的默认全文 analyzer 就是一套固定的 simple + 3-gram 风格切词**
- **而不是一个可配置 analyzer 框架**

### 6.4 JSON / JSON_VALUE

对于 JSON：

- `json`: 先枚举 JSON value，再对 value 内文本继续做 simple tokenize
- `json_value`: 直接把 JSON value 当 token 输出

当前调用的是 `TokenizeValue(false)`，也就是：

- **只索引 value，不索引 key**

### 6.5 datalink

如果索引列是 `datalink`，`fulltext_index_tokenize` 会先取纯文本，再继续切词。

这会把外部文本抽取的 I/O 和全文索引构建绑在一起，进一步放大写路径成本。

---

## 7. 查询改写链路

### 7.1 binder 之后是什么

`MATCH(body, title) AGAINST('red')` 在 binder 之后会被改成内部函数：

```text
fulltext_match(pattern, mode, body, title)
```

### 7.2 optimizer 怎么处理

`apply_indices_fulltext.go` 会：

1. 在 `FilterList` / `ProjectList` 中寻找 `fulltext_match`
2. 检查是否存在**列集合完全匹配**的 FULLTEXT index
3. 如果找到：
   - 构造 `fulltext_index_scan(...)` table function
   - 让返回结果与原表按 `doc_id = pk` 做 INNER JOIN
   - 在上层添加 `score DESC` 排序
   - 把 projection 里的 `fulltext_match()` 替换成 score 列

### 7.3 不匹配时会怎样

如果没有匹配的 FULLTEXT index：

- `fullTextMatch()` / `fullTextMatchScore()` 直接返回 `NotSupported`
- **不会退化成全表扫描全文匹配**

所以当前并不是：

- “有索引走快路径，没索引走慢路径”

而是：

- **“有对应全文索引才能执行；没有就直接报不支持”**

这也是测试里 `match(body)` 但只建了 `(body, title)` 索引时直接报错的原因。

---

## 8. `fulltext_index_scan` 到底在做什么

### 8.1 第一步：解析搜索串

查询开始时，先调用 `ParsePattern()` 把搜索串变成内部 pattern tree。

支持的布尔操作包括：

- `+`
- `-`
- `~`
- `<`
- `>`
- `*`
- phrase
- group

代码位置：

- `pkg/fulltext/fulltext.go`

### 8.2 第二步：把 pattern 转成 SQL

`pkg/fulltext/sql.go` 会根据 pattern 动态生成 SQL。

关键特点：

- 单词查询：`word = 'xxx'`
- 前缀查询：`prefix_eq(word, 'xxx')`
- phrase / proximity：通过多个子查询按 `doc_id` 和 `pos` 差值 join
- boolean query：大量使用 CTE、JOIN、`UNION ALL`

代码里还直接写了注释：

- 不要用 `ORDER BY`
- 不要用 `UNION`
- 否则会慢或 OOM

这其实已经说明当前 SQL 生成方式本身就很脆弱。

### 8.3 第三步：每次查询先数一遍语料

`fulltext_index_scan` 会先执行：

```sql
SELECT COUNT(*), AVG(pos)
FROM index_table
WHERE word = '__DocLen'
```

这里的 `__DocLen` 不是正常词，而是切词时额外插入的一条“文档长度伪词”记录。

也就是说：

- **每次查询都要额外统计全文文档数和平均文档长度**
- **BM25 的全局统计不是预计算元数据，而是查询时动态拿**

### 8.4 第四步：流式拉候选，再在 Go 内存里聚合

SQL 结果返回 `(doc_id, keyword_index[, doc_len])` 后，`groupby()` 会：

1. 按 `doc_id` 建哈希表
2. 给每个文档构造一个 `[]uint8` 的词频向量
3. 再维护全局 `aggcnt[]` 统计每个关键词命中的文档数

然后 `evaluate()` / `sort_topk()` 再在 Go 里做：

- TF-IDF
- BM25
- top-k heap

这意味着查询执行模型是：

- **SQL 做粗候选召回**
- **Go 做主聚合和打分**

而不是：

- **底层倒排索引直接按 posting list 算交并 / 词频 / top-k**

### 8.5 LIMIT 并没有真正省掉多少工作

虽然 planner 会把 limit 挂到 `fulltext_index_scan`，执行器里也会走 heap top-k，
但它仍然要先把候选文档流式聚合进内存，再统一算分。

所以当前 limit 的价值主要是：

- 限制最后输出

而不是：

- 显著减少候选生成
- 显著减少 posting 读取
- 提前终止大部分计算

---

## 9. 语义层面的几个关键问题

### 9.1 `natural language mode` 的实现偏离主流

这是当前实现最值得特别指出的问题之一。

`patternToSql()` 对 `FULLTEXT_NL` / 默认模式走的是：

```go
return SqlPhrase(ps, mode, idxtbl, true)
```

也就是说，**当前的 natural language mode 实际上走的是 phrase/proximity SQL 路径**，而不是主流全文系统中的：

- 先分析 query
- 再做 bag-of-words 召回
- 再做 BM25/TF-IDF 相关性排序

这会带来几个直接后果：

1. 用户以为自己在做“自然语言全文检索”，实际却更接近“按 token 顺序和位置严格匹配”
2. 对中文 3-gram token 序列尤其敏感
3. 召回行为和 MySQL / ES / Lucene 的直觉不同

### 9.2 phrase 候选能过滤，但没有独立的 proximity ranking

显式 phrase 查询（例如 `"foo bar"`）在 SQL 侧会通过 `pos` 差值做过滤，所以候选集顺序约束是生效的。  
但 `EvalPhrase()` 仍处于注释状态，说明它没有单独实现更精细的 phrase/proximity ranking。

更准确地说，当前 phrase 更像：

- **SQL 过滤保证顺序**
- **评分仍然走比较简单的词频/文长路径**

### 9.3 `query expansion` 只是 parser 支持，不是执行支持

这类“语法看起来有，执行实际没有”的能力，会直接造成用户预期错位。

### 9.4 parser 名字与实际能力不对齐

`ngram`、`default`、`json`、`json_value` 在 DDL 层看起来像 analyzer 体系，
但真正可配置的 analyzer pipeline 还不存在。

---

## 10. 为什么数据量大时体验会很差

## 10.1 根因不是一个点，而是整条链路都偏重

当前实现对大数据量不友好的根因，可以概括为：

### A. 写路径重

- 建索引是同步整表回填
- UPDATE 需要整文档删后重建
- datalink / json 还会引入额外解析开销

### B. 查询路径重

- 先 count/avg 统计
- 再动态生成 SQL
- 再流式拉候选
- 再在 Go 里用 map 聚合
- 再算 TF-IDF/BM25
- 再 top-k

### C. 索引结构弱

- 没有 segment
- 没有 posting compression
- 没有 skip data
- 没有词典层快速跳转
- 没有 delete bitmap
- 没有后台 merge

### D. analyzer / relevance 弱

- 默认分析器太简单
- 没有语言学能力
- natural language 语义不对
- BM25 不是默认

## 10.2 这意味着“完全不能用”吗

更准确的说法是：

- **不是一点也不能用**
- **但它更像功能型实现 / 小规模能力验证，而不是大规模生产级全文检索**

如果场景是：

- 数据量不大
- 文本不长
- 写入频率不高
- 查询并发不高
- 对召回/排序质量要求不高

那它可以工作。

但如果场景是：

- 大量文档
- 高频更新
- 高并发查询
- 要求稳定 top-k latency
- 要求接近主流搜索体验

那当前实现会很快暴露问题。

## 10.3 一个更直白的定位

**当前实现更接近“数据库里做了全文功能”，还不是“数据库里做成了搜索引擎级全文索引”。**

---

## 11. 现状结论

### 11.1 当前版本最适合怎么描述

建议对外把当前实现描述成：

- 已具备 SQL 级 FULLTEXT 语法和基础检索能力
- 可用于功能验证、Demo、小规模应用
- 仍处在从“功能可用”走向“生产可用”的阶段

### 11.2 不建议怎么描述

不建议把它描述成：

- 已经是大规模全文搜索方案
- 等同于 MySQL InnoDB FTS
- 等同于 Elasticsearch / Lucene

### 11.3 对后续演进的核心判断

如果 MO 真要把全文检索做成长期能力，**必须从“隐藏 token 表 + 查询时聚合”升级到“engine-native、段式、可增量、可 merge、可预计算统计”的倒排索引架构。**

这也是下一份方案文档的核心方向。
