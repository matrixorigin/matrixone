# 全文检索 + 向量索引

## 全文检索（`pkg/fulltext/`）

- **核心类型：**
  - `FullTextScoreAlgo` — 相关性评分算法
  - `FullTextParserParam` — 解析配置
  - `FullTextBooleanOperator` — 布尔查询算子（AND/OR/NOT）

- **SQL 语法：**
```sql
CREATE FULLTEXT INDEX idx ON t(col);
SELECT * FROM t WHERE MATCH(col) AGAINST('keyword' IN BOOLEAN MODE);
```

### JSON Parser 合同（`WITH PARSER json`）

`FULLTEXT2 ... WITH PARSER json` 可以为 JSON 文档建立全文索引；它不把普通 JSON
函数谓词变成一项已承诺的索引访问能力。

- `json_extract`、`json_extract_string` 和 `json_extract_float64` 仍按普通 SQL 谓词
  保持精确的关系语义。带 JSON 全文索引的表与不带该索引的同一查询必须返回相同的行集。
- 当前读不承诺将上述 JSON 谓词自动改写为 `fulltext2_search`，也不承诺 `EXPLAIN`
  中出现特定的全文索引节点。看到 `Table Scan + Filter` 是允许且正确的回退计划。
- JSON 全文索引的异步维护尚不能证明覆盖当前读快照时，优化器必须保留原始 JSON
  谓词并回退扫描；不能为了获得索引加速而返回不完整结果。
- 因此，针对普通 JSON 谓词的测试以结果与无索引 SQL oracle 一致为准，不能仅以
  是否命中全文索引计划判定成功或失败。JSON current-read probe 加速属于独立 Feature，
  不是现有 JSON parser 的支持承诺。

该边界与 [#27926 的维护者说明](https://github.com/matrixorigin/matrixone/issues/27926#issuecomment-5506759750)
一致：该 probe 功能尚未提供，未走该路径不是产品 Bug。

## 向量索引（`pkg/vectorindex/`）

- **核心类型：**
  - `IndexTableConfig` — 索引配置
  - `IvfflatIndexConfig` — IVF-Flat 算法配置
  - `VectorIndexCdc[T]` — 向量索引的 CDC 更新
  - `SearchResultIf` — 搜索结果接口

- **支持算法：** IVF-Flat, HNSW

- **SQL 语法：**
```sql
CREATE INDEX idx USING IVFFLAT ON t(embedding_col) LISTS = 100;
SELECT * FROM t ORDER BY l2_distance(embedding_col, '[1,2,3]') LIMIT 10;
```

## 向量化层（`pkg/vectorize/`）

- 表达式向量化执行
- 批量向量运算优化

## 与测试的关联

| 变更范围 | 影响的测试 |
|---------|----------|
| 全文检索 | BVT: fulltext; 稳定性: fulltext-vector |
| 向量索引 | BVT: vector; 稳定性: vector IVF+DML concurrency |
| 向量索引 CDC | Chaos: fulltext 故障场景 |
| 评分算法 | BVT: fulltext |
