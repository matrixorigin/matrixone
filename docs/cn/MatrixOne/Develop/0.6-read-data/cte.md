# 公共表表达式 (CTE)

公用表表达式（CTE,Common table expression)是一个命名的临时结果集，仅在单个 SQL 语句(例如 `SELECT`，`INSERT`，`UPDATE` 或 `DELETE`)的执行范围内存在。

与派生表类似，CTE 不作为对象存储，仅在查询执行期间持续；与派生表不同，CTE 可以是自引用(递归 CTE，暂时不支持)，也可以在同一查询中多次引用。 此外，与派生表相比，CTE 提供了更好的可读性和性能。

CTE 的结构包括名称，可选列列表和定义 CTE 的查询。

CTE 语法如下：

```sql
WITH <query_name> AS (
    <query_definition>
)
SELECT ... FROM <query_name>;
```

## 开始前准备

你需要确认在开始之前，已经完成了以下任务：

- 已通过[源代码](https://docs.matrixorigin.io/cn/0.5.1/MatrixOne/Get-Started/install-standalone-matrixone/#1)或[二进制包](https://docs.matrixorigin.io/cn/0.5.1/MatrixOne/Get-Started/install-standalone-matrixone/#2)完成安装 MatrixOne
- 已完成[连接 MatrixOne 服务](../../Get-Started/connect-to-matrixone-server.md)

### 数据准备

你可以新建一个简单的表，插入一些数据，帮助你理解后续所展示的 CTE 语句：

```sql
> drop table if exists t1;
> create table t1(a int, b int, c int);
> insert into t1 values(null,null,null),(2,3,4);
```

## CTE 语句使用实例

在下面的示例中，`qn` 作为一个临时的结果集被创建，此时相应的查询结果会被缓存在 MatrixOne 中，你在执行正式的 `qn` 查询时，比非 CTE 场景的性能有所提升。

```sql
WITH qn AS (SELECT a FROM t1), qn2 as (select b from t1)
SELECT * FROM qn;
```

查询结果如下：

```
+------+
| a    |
+------+
| NULL |
|    2 |
+------+
2 rows in set (0.00 sec)
```
