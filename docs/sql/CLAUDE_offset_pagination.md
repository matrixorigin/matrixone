# ORDER BY 与稳定分页

## 非唯一排序键不保证跨页稳定

下面的查询只规定按 `ts` 排序，没有规定相同时间戳的行之间如何排序：

```sql
SELECT id FROM events ORDER BY ts LIMIT 30;
SELECT id FROM events ORDER BY ts LIMIT 30 OFFSET 30;
```

如果多行的 `ts` 相同，两次查询可能选用不同的等键顺序，导致相邻页重复出现某些行，另一些行没有被遍历到。这不表示表中的数据被删除，也不表示单个查询返回了重复的主键。

即使同一条分页查询执行多次都得到相同结果，也不能据此认为不同 `LIMIT` / `OFFSET` 的查询共享同一个顺序。Top-N 的候选范围、执行计划、并行扫描和归并方式都可能影响等键行的顺序。不能依赖主键的存储顺序、插入顺序或某次执行中观察到的顺序。

MySQL 同样不保证非唯一排序键下的等键顺序，参见 [MySQL LIMIT Optimization](https://dev.mysql.com/doc/refman/8.0/en/limit-optimization.html)。

## 为排序补充唯一键

若 `id` 是主键，在所有分页查询中明确使用同一套完整排序键：

```sql
CREATE TABLE events (
    id INT PRIMARY KEY,
    ts DATETIME NOT NULL,
    KEY i_ts (ts)
);

SELECT id, ts FROM events ORDER BY ts, id LIMIT 30;
SELECT id, ts FROM events ORDER BY ts, id LIMIT 30 OFFSET 30;

-- 倒序分页同样需要唯一的次级排序键。
SELECT id, ts FROM events ORDER BY ts DESC, id DESC LIMIT 30;
SELECT id, ts FROM events ORDER BY ts DESC, id DESC LIMIT 30 OFFSET 30;
```

在数据集不变、各页使用同一可见快照时，唯一的 `(ts, id)` 元组定义全序，OFFSET 才能稳定地截取对应区间。复合主键应补齐足以唯一标识结果行的键；JOIN 的结果可能包含同一表主键的多个匹配行，不能直接假定单表主键在结果中仍然唯一。

## 游标分页（keyset pagination）

深分页可以使用上一页最后一行的完整排序键作为游标，而不是反复跳过越来越多的行。以下示例适用于 `ts` 和 `id` 均非 NULL，且两个键均升序的情况；应用需要保存上一页末行的时间戳和 ID：

```sql
-- 假设上一页末行的游标为 ('2026-01-01 00:00:00', 5)。
SELECT id, ts
FROM events
WHERE ts > '2026-01-01 00:00:00'
   OR (ts = '2026-01-01 00:00:00' AND id > 5)
ORDER BY ts, id
LIMIT 30;
```

两个键均倒序时，使用 `<` 和 `ORDER BY ts DESC, id DESC`。混合升降序或允许 NULL 的键需要与实际排序规则一致的游标条件，不能直接套用上述比较式。

唯一排序键只解决等键顺序问题，不会冻结数据：

- 不同分页请求之间的插入、删除或排序键更新会改变行的位置，OFFSET 仍可能重漏。
- 游标分页避免依赖前面已有多少行，但也不等于固定快照；排序键更新等并发变化仍可能影响遍历。
- 如果要求完整遍历同一份数据，应让各页查询使用同一可见快照，并避免遍历期间自行修改该数据集。不要把“使用相同连接”当成“使用相同快照”的保证。

## 回归覆盖

[`offset_pagination.sql`](../../test/distributed/cases/dml/select/offset_pagination.sql) 使用 8 行数据和两个重复时间戳，验证带唯一排序键的升降序分页、尾页/空页、预处理参数复用和游标条件。非唯一排序键的对照只验证时间戳与行数，不固定等键行的 ID 顺序。
