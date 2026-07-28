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
