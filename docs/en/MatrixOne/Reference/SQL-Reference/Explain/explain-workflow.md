# EXPLAIN Output Format

## Output Structure

The command's result is a textual description of the plan selected for the *`statement`*, optionally annotated with execution statistics.

Take the following SQL as an example, we demonstrate the output structure.

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

EXPLAIN outputs a tree structure, named as `Execution Plan Tree`. Every leaf node includes the information of node type, affected objects and other properties such as `cost`, `rowsize` etc. We can simplify the above example only with node type information. It visualizes the whole process of a SQL query,  shows which operation nodes it goes through and what are their cost  estimation.

```
Project
└── Aggregate
    └── Join
        └── Table Scan
        └──	Join
        	  └──Table Scan
        	  └──Table Scan
```

## Node types

In 0.5.0 version, the following node types are supported.

| Node Type       | Name in Explain |
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

| Property    | Format                                                       | Description                                                  |
| ----------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| cost        | cost=0.00..0.00                                              | The first is estimated start-up cost. This is the time expended before the output phase can begin, e.g., time to do the sorting in a sort node. The second is estimated total cost. This is stated on the assumption that the plan node is run to completion, i.e., all available rows are retrieved. In practice a node's parent node might stop short of reading all available rows (see the `LIMIT` example below). |
| card        | card=14.00                                                   | Estimated column cardinality.                                |
| ndv         | ndv=0.00                                                     | Estimated number of distinct values.                         |
| rowsize     | rowsize=0.00                                                 | Estimated rowsize.                                           |
| output      | Output: #[0,0], #[0,1], #[0,2], #[0,3], #[0,4], #[0,5], #[0,6], #[0,7] | Node output information.                                     |
| Table       | Table : 'emp' (0:'empno', 1:'ename', 2:'job', 3:'mgr',)      | Table definition information after column pruning.           |
| Filter Cond | Filter Cond: (CAST(#[0,5] AS DECIMAL128) > CAST(20 AS DECIMAL128)) | Filter condition.                                            |

### Values Scan

| Property | Format                                          | Description             |
| -------- | ----------------------------------------------- | ----------------------- |
| cost     | (cost=0.00..0.00 card=14.00 ndv=0.00 rowsize=0) | Estimated cost          |
| output   | Output: 0                                       | Node output information |

### Project

| Property | Format                                          | Description             |
| -------- | ----------------------------------------------- | ----------------------- |
| cost     | (cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0) | Estimated cost          |
| output   | Output: (CAST(#[0,0] AS INT64) + 2)             | Node output information |

### Aggregate

| Property            | Format                                                       | Description             |
| ------------------- | ------------------------------------------------------------ | ----------------------- |
| cost                | (cost=0.00..0.00 card=14.00 ndv=0.00 rowsize=0)              | Estimated cost          |
| output              | Output: #[0,0], #[0,1], #[0,2], #[0,3], #[0,4], #[0,5], #[0,6], #[0,7] | Node output information |
| Group Key           | Group Key:#[0,0]                                             | Key for grouping        |
| Aggregate Functions | Aggregate Functions: max(#[0,1])                             | Aggregate function name |

### Filter

| Property    | Format                                                       | Description             |
| ----------- | ------------------------------------------------------------ | ----------------------- |
| cost        | (cost=0.00..0.00 card=14.00 ndv=0.00 rowsize=0)              | Estimated cost          |
| output      | Output: #[0,0], #[0,1], #[0,2], #[0,3], #[0,4], #[0,5], #[0,6], #[0,7] | Node output information |
| Filter Cond | Filter Cond: (CAST(#[0,1] AS INT64) > 10)                    | Filter condition        |

### Join

| Property         | Format                                          | Description             |
| ---------------- | ----------------------------------------------- | ----------------------- |
| cost             | (cost=0.00..0.00 card=14.00 ndv=0.00 rowsize=0) | Estimated cost          |
| output           | Output: #[0,0]                                  | Node output information |
| Join Type: INNER | Join Type: INNER                                | Join type               |
| Join Cond        | Join Cond: (#[0,0] = #[1,0])                    | Join condition          |

### Sort

| Property | Format                                                       | Description                   |
| -------- | ------------------------------------------------------------ | ----------------------------- |
| cost     | (cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0)              | Estimated cost                |
| output   | Output: #[0,0], #[0,1], #[0,2], #[0,3], #[0,4], #[0,5], #[0,6], #[0,7] | Node output information       |
| Sort Key | Sort Key: #[0,0] DESC,  #[0,1] INTERNAL                      | Sort key                      |
| Limit    | Limit: 10                                                    | Number limit for output data  |
| Offset   | Offset: 20                                                   | Number offset for output data |
