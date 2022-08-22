# EXPLAIN 输出格式

## 输出结构

语法结构执行结果是为 `statement` 选择的计划的文本描述，可以选择使用执行统计信息进行注释。

以下以 SQL 为例，演示输出结构：

```
explain select city,libname1,count(libname1) as a from t3 join t1 on libname1=libname3 join t2 on isbn3=isbn2 group by city,libname1;
```

```
+--------------------------------------------------------------------------------------------+
| QUERY PLAN                                                                                 |
+--------------------------------------------------------------------------------------------+
| Project(cost=0.00..0.00 card=400.00 ndv=0.00 rowsize=0                                     |
|   ->  Aggregate(cost=0.00..0.00 card=400.00 ndv=0.00 rowsize=0                             |
|         Group Key:#[0,1], #[0,0]                                                           |
|         Aggregate Functions: count(#[0,0])                                                 |
|         ->  Join(cost=0.00..0.00 card=400.00 ndv=0.00 rowsize=0                            |
|               Join Type: INNER                                                             |
|               Join Cond: (#[1,2] = #[0,0])                                                 |
|               ->  Table Scan on abc.t2(cost=0.00..0.00 card=8.00 ndv=0.00 rowsize=0        |
|               ->  Join(cost=0.00..0.00 card=50.00 ndv=0.00 rowsize=0                       |
|                     Join Type: INNER                                                       |
|                     Join Cond: (#[0,0] = #[1,1])                                           |
|                     ->  Table Scan on abc.t1(cost=0.00..0.00 card=5.00 ndv=0.00 rowsize=0  |
|                     ->  Table Scan on abc.t3(cost=0.00..0.00 card=10.00 ndv=0.00 rowsize=0 |
+--------------------------------------------------------------------------------------------+
13 rows in set (0.00 sec)
```

EXPLAIN 输出一个名称为 `Execution Plan Tree` 树形结构，每个叶子节点都包含节点类型、受影响的对象以及其他属性的信息，如 `cost`， `rowsize` 等。我们现在只使用节点类型信息来简化展示上面的示例。`Execution Plan Tree` 树形结构可以可视化 SQL 查询的整个过程，显示它所经过的操作节点以及它们的成本估计。

```
Project
└── Aggregate
    └── Join
        └── Table Scan
        └──	Join
        	  └──Table Scan
        	  └──Table Scan
```

## 节点类型

MatrixOne 0.5.0 版本支持以下节点类型。

| 节点类型       | Explain 中的命名 |
| --------------- | --------------- |
| Node_VALUE_SCAN | Values Scan     |
| Node_TABLE_SCAN | Table Scan      |
| Node_PROJECT    | Project         |
| Node_AGG        | Aggregate       |
| Node_FILTER     | Filter          |
| Node_JOIN       | Join            |
| Node_SORT       | Sort            |
| Node_INSERT     | Insert          |
| Node_UPDATE     | Update          |
| Node_DELETE     | Delete          |

### Table Scan

| 特性    | 格式                                                      | 描述                                                  |
| ----------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| cost        | cost=0.00..0.00                                              | The first is estimated start-up cost. This is the time expended before the output phase can begin, e.g., time to do the sorting in a sort node. The second is estimated total cost. This is stated on the assumption that the plan node is run to completion, i.e., all available rows are retrieved. In practice a node's parent node might stop short of reading all available rows (see the `LIMIT` example below). |
| card        | card=14.00                                                   | Estimated column cardinality.                                |
| ndv         | ndv=0.00                                                     | Estimated number of distinct values.                         |
| rowsize     | rowsize=0.00                                                 | Estimated rowsize.                                           |
| output      | Output: #[0,0], #[0,1], #[0,2], #[0,3], #[0,4], #[0,5], #[0,6], #[0,7] | Node output information.                                     |
| Table       | Table : 'emp' (0:'empno', 1:'ename', 2:'job', 3:'mgr',)      | Table definition information after column pruning.           |
| Filter Cond | Filter Cond: (CAST(#[0,5] AS DECIMAL128) > CAST(20 AS DECIMAL128)) | Filter condition.                                            |

### Values Scan

| 特性 | 格式                                          | 描述             |
| -------- | ----------------------------------------------- | ----------------------- |
| cost     | (cost=0.00..0.00 card=14.00 ndv=0.00 rowsize=0) | Estimated cost          |
| output   | Output: 0                                       | Node output information |

### Project

| 特性 | 格式                                          | 描述            |
| -------- | ----------------------------------------------- | ----------------------- |
| cost     | (cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0) | Estimated cost          |
| output   | Output: (CAST(#[0,0] AS INT64) + 2)             | Node output information |

### Aggregate

| 特性            | 格式                                                     | 描述            |
| ------------------- | ------------------------------------------------------------ | ----------------------- |
| cost                | (cost=0.00..0.00 card=14.00 ndv=0.00 rowsize=0)              | Estimated cost          |
| output              | Output: #[0,0], #[0,1], #[0,2], #[0,3], #[0,4], #[0,5], #[0,6], #[0,7] | Node output information |
| Group Key           | Group Key:#[0,0]                                             | Key for grouping        |
| Aggregate Functions | Aggregate Functions: max(#[0,1])                             | Aggregate function name |

### Filter

| 特性    | 格式                                                      | 描述            |
| ----------- | ------------------------------------------------------------ | ----------------------- |
| cost        | (cost=0.00..0.00 card=14.00 ndv=0.00 rowsize=0)              | Estimated cost          |
| output      | Output: #[0,0], #[0,1], #[0,2], #[0,3], #[0,4], #[0,5], #[0,6], #[0,7] | Node output information |
| Filter Cond | Filter Cond: (CAST(#[0,1] AS INT64) > 10)                    | Filter condition        |

### Join

| 特性         | 格式                                          | 描述            |
| ---------------- | ----------------------------------------------- | ----------------------- |
| cost             | (cost=0.00..0.00 card=14.00 ndv=0.00 rowsize=0) | Estimated cost          |
| output           | Output: #[0,0]                                  | Node output information |
| Join Type: INNER | Join Type: INNER                                | Join type               |
| Join Cond        | Join Cond: (#[0,0] = #[1,0])                    | Join condition          |

### Sort

| 特性 | 格式                                                      | 描述                  |
| -------- | ------------------------------------------------------------ | ----------------------------- |
| cost     | (cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0)              | Estimated cost                |
| output   | Output: #[0,0], #[0,1], #[0,2], #[0,3], #[0,4], #[0,5], #[0,6], #[0,7] | Node output information       |
| Sort Key | Sort Key: #[0,0] DESC,  #[0,1] INTERNAL                      | Sort key                      |
| Limit    | Limit: 10                                                    | Number limit for output data  |
| Offset   | Offset: 20                                                   | Number offset for output data |
