## EXPLAIN

EXPLAIN — show the execution plan of a statement

## Syntax

```
EXPLAIN [ ( option [, ...] ) ] statement

where option can be one of:
    VERBOSE [ boolean ]
    FORMAT { TEXT | JSON }
```

For 0.5.0, only `TEXT` format is supported.

## Description

This command displays the execution plan that the MatrixOne planner generates for the supplied statement. The execution plan shows how the table(s) referenced by the statement will be scanned — by plain sequential scan, index scan, etc. — and if multiple tables are referenced, what join algorithms will be used to bring together the required rows from each input table.

The most critical part of the display is the estimated statement execution cost, which is the planner's guess at how long it will take to run the statement (measured in cost units that are arbitrary, but conventionally mean disk page fetches). Actually two numbers are shown: the start-up cost before the first row can be returned, and the total cost to return all the rows. For most queries the total cost is what matters, but in contexts such as a subquery in `EXISTS`, the planner will choose the smallest start-up cost instead of the smallest total cost (since the executor will stop after getting one row, anyway). Also, if you limit the number of rows to return with a `LIMIT` clause, the planner makes an appropriate interpolation between the endpoint costs to estimate which plan is really the cheapest.

## Parameters

* VERBOSE:

Display additional information regarding the plan. Specifically, include the output column list for each node in the plan tree, schema-qualify table and function names, always label variables in expressions with their range table alias, and always print the name of each trigger for which statistics are displayed. This parameter is `FALSE` by default.

* FORMAT:

Specify the output format, which can be TEXT, JSON. Non-text output contains the same information as the text output format, but is easier for programs to parse. This parameter is `TEXT` by dafault.

* BOOLEAN:

Specifies whether the selected option should be turned on or off. You can write `TRUE`to enable the option, and `FALSE` to disable it. The *`boolean`* value can also be omitted, in which case `TRUE` is assumed.

* STETEMENT

MatrixOne supports any `SELECT`, `UPDATE`, `DELETE` statement execution plan. For `INSERT` statement, only `INSERT INTO..SELECT` is supported in 0.5.0 version. `INSERT INTO...VALUES` is not supported yet.

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

#### Table Scan

| Property    | Format                                                       | Description                                                  |
| ----------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| cost        | cost=0.00..0.00                                              | The first is estimated start-up cost. This is the time expended before the output phase can begin, e.g., time to do the sorting in a sort node. The second is estimated total cost. This is stated on the assumption that the plan node is run to completion, i.e., all available rows are retrieved. In practice a node's parent node might stop short of reading all available rows (see the `LIMIT` example below). |
| card        | card=14.00                                                   | Estimated column cardinality.                                |
| ndv         | ndv=0.00                                                     | Estimated number of distinct values.                         |
| rowsize     | rowsize=0.00                                                 | Estimated rowsize.                                           |
| output      | Output: #[0,0], #[0,1], #[0,2], #[0,3], #[0,4], #[0,5], #[0,6], #[0,7] | Node output information.                                     |
| Table       | Table : 'emp' (0:'empno', 1:'ename', 2:'job', 3:'mgr',)      | Table definition information after column pruning.           |
| Filter Cond | Filter Cond: (CAST(#[0,5] AS DECIMAL128) > CAST(20 AS DECIMAL128)) | Filter condition.                                            |

#### Values Scan

| Property | Format                                          | Description             |
| -------- | ----------------------------------------------- | ----------------------- |
| cost     | (cost=0.00..0.00 card=14.00 ndv=0.00 rowsize=0) | Estimated cost          |
| output   | Output: 0                                       | Node output information |

#### Project

| Property | Format                                          | Description             |
| -------- | ----------------------------------------------- | ----------------------- |
| cost     | (cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0) | Estimated cost          |
| output   | Output: (CAST(#[0,0] AS INT64) + 2)             | Node output information |

#### Aggregate

| Property            | Format                                                       | Description             |
| ------------------- | ------------------------------------------------------------ | ----------------------- |
| cost                | (cost=0.00..0.00 card=14.00 ndv=0.00 rowsize=0)              | Estimated cost          |
| output              | Output: #[0,0], #[0,1], #[0,2], #[0,3], #[0,4], #[0,5], #[0,6], #[0,7] | Node output information |
| Group Key           | Group Key:#[0,0]                                             | Key for grouping        |
| Aggregate Functions | Aggregate Functions: max(#[0,1])                             | Aggregate function name |

#### Filter

| Property    | Format                                                       | Description             |
| ----------- | ------------------------------------------------------------ | ----------------------- |
| cost        | (cost=0.00..0.00 card=14.00 ndv=0.00 rowsize=0)              | Estimated cost          |
| output      | Output: #[0,0], #[0,1], #[0,2], #[0,3], #[0,4], #[0,5], #[0,6], #[0,7] | Node output information |
| Filter Cond | Filter Cond: (CAST(#[0,1] AS INT64) > 10)                    | Filter condition        |

#### Join

| Property         | Format                                          | Description             |
| ---------------- | ----------------------------------------------- | ----------------------- |
| cost             | (cost=0.00..0.00 card=14.00 ndv=0.00 rowsize=0) | Estimated cost          |
| output           | Output: #[0,0]                                  | Node output information |
| Join Type: INNER | Join Type: INNER                                | Join type               |
| Join Cond        | Join Cond: (#[0,0] = #[1,0])                    | Join condition          |

#### Sort

| Property | Format                                                       | Description                   |
| -------- | ------------------------------------------------------------ | ----------------------------- |
| cost     | (cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0)              | Estimated cost                |
| output   | Output: #[0,0], #[0,1], #[0,2], #[0,3], #[0,4], #[0,5], #[0,6], #[0,7] | Node output information       |
| Sort Key | Sort Key: #[0,0] DESC,  #[0,1] INTERNAL                      | Sort key                      |
| Limit    | Limit: 10                                                    | Number limit for output data  |
| Offset   | Offset: 20                                                   | Number offset for output data |

## Examples

#### Node_TABLE_SCAN

```sql
mysql> explain verbose SELECT N_NAME, N_REGIONKEY a FROM NATION WHERE N_NATIONKEY > 0 OR N_NATIONKEY < 10;
+------------------------------------------------------------------------------------+
| QUERY PLAN                                                                         |
+------------------------------------------------------------------------------------+
| Project(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0)                             |
|   Output: #[0,0], #[0,1]                                                           |
|   ->  Table Scan on db1.nation(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0)      |
|         Output: #[0,1], #[0,2]                                                     |
|         Table: 'nation' (0:'n_nationkey', 1:'n_name', 2:'n_regionkey')             |
|         Filter Cond: ((CAST(#[0,0] AS INT64) > 0) or (CAST(#[0,0] AS INT64) < 10)) |
+------------------------------------------------------------------------------------+
```

#### Node_VALUE_SCAN

```sql
mysql> explain verbose select abs(-1);
+-----------------------------------------------------------------------------+
| QUERY PLAN                                                                  |
+-----------------------------------------------------------------------------+
| Project(cost=0.00..0.00 card=1.00 ndv=0.00 rowsize=0)                       |
|   Output: 1                                                                 |
|   ->  Values Scan "*VALUES*" (cost=0.00..0.00 card=1.00 ndv=0.00 rowsize=0) |
|         Output: 0                                                           |
+-----------------------------------------------------------------------------+
```

#### Node_SORT

```sql
mysql> explain verbose SELECT N_NAME, N_REGIONKEY a FROM NATION WHERE N_NATIONKEY > 0 AND N_NATIONKEY < 10 ORDER BY N_NAME, N_REGIONKEY DESC;
+--------------------------------------------------------------------------------------------+
| QUERY PLAN                                                                                 |
+--------------------------------------------------------------------------------------------+
| Project(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0)                                     |
|   Output: #[0,0], #[0,1]                                                                   |
|   ->  Sort(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0)                                  |
|         Output: #[0,0], #[0,1]                                                             |
|         Sort Key: #[0,0] INTERNAL,  #[0,1] DESC                                            |
|         ->  Project(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0)                         |
|               Output: #[0,0], #[0,1]                                                       |
|               ->  Table Scan on db1.nation(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0)  |
|                     Output: #[0,1], #[0,2]                                                 |
|                     Table: 'nation' (0:'n_nationkey', 1:'n_name', 2:'n_regionkey')         |
|                     Filter Cond: (CAST(#[0,0] AS INT64) > 0), (CAST(#[0,0] AS INT64) < 10) |
+--------------------------------------------------------------------------------------------+
```

With limit and offset:

```sql
mysql> explain SELECT N_NAME, N_REGIONKEY FROM NATION WHERE abs(N_REGIONKEY) > 0 AND N_NAME LIKE '%AA' ORDER BY N_NAME DESC, N_REGIONKEY limit 10;
+-------------------------------------------------------------------------------------------+
| QUERY PLAN                                                                                |
+-------------------------------------------------------------------------------------------+
| Project(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0)                                    |
|   ->  Sort(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0)                                 |
|         Sort Key: #[0,0] DESC,  #[0,1] INTERNAL                                           |
|         Limit: 10                                                                         |
|         ->  Project(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0)                        |
|               ->  Table Scan on db1.nation(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0) |
|                     Filter Cond: (abs(CAST(#[0,1] AS INT64)) > 0), (#[0,0] like '%AA')    |
+-------------------------------------------------------------------------------------------+

```

```sql
mysql> explain SELECT N_NAME, N_REGIONKEY FROM NATION WHERE abs(N_REGIONKEY) > 0 AND N_NAME LIKE '%AA' ORDER BY N_NAME DESC, N_REGIONKEY LIMIT 10 offset 20;
+-------------------------------------------------------------------------------------------+
| QUERY PLAN                                                                                |
+-------------------------------------------------------------------------------------------+
| Project(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0)                                    |
|   ->  Sort(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0)                                 |
|         Sort Key: #[0,0] DESC,  #[0,1] INTERNAL                                           |
|         Limit: 10, Offset: 20                                                             |
|         ->  Project(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0)                        |
|               ->  Table Scan on db1.nation(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0) |
|                     Filter Cond: (abs(CAST(#[0,1] AS INT64)) > 0), (#[0,0] like '%AA')    |
+-------------------------------------------------------------------------------------------+
```

#### Node_AGG

```sql
mysql> explain verbose SELECT count(*) FROM NATION group by N_NAME;
+-------------------------------------------------------------------------------------+
| QUERY PLAN                                                                          |
+-------------------------------------------------------------------------------------+
| Project(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0)                              |
|   Output: #[0,0]                                                                    |
|   ->  Aggregate(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0)                      |
|         Output: #[-2,0]                                                             |
|         Group Key:#[0,1]                                                            |
|         Aggregate Functions: starcount(#[0,0])                                      |
|         ->  Table Scan on db1.nation(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0) |
|               Output: #[0,0], #[0,1]                                                |
|               Table: 'nation' (0:'n_nationkey', 1:'n_name')                         |
+-------------------------------------------------------------------------------------+
```

#### Node_JOIN

```sql
mysql> explain verbose SELECT NATION.N_NAME, REGION.R_NAME FROM NATION join REGION on NATION.N_REGIONKEY = REGION.R_REGIONKEY WHERE NATION.N_REGIONKEY > 10 AND LENGTH(NATION.N_NAME) > LENGTH(REGION.R_NAME);
+--------------------------------------------------------------------------------------------+
| QUERY PLAN                                                                                 |
+--------------------------------------------------------------------------------------------+
| Project(cost=0.00..0.00 card=125.00 ndv=0.00 rowsize=0)                                    |
|   Output: #[0,1], #[0,0]                                                                   |
|   ->  Filter(cost=0.00..0.00 card=125.00 ndv=0.00 rowsize=0)                               |
|         Output: #[0,0], #[0,1]                                                             |
|         Filter Cond: (length(CAST(#[0,1] AS CHAR)) > length(CAST(#[0,0] AS CHAR)))         |
|         ->  Join(cost=0.00..0.00 card=125.00 ndv=0.00 rowsize=0)                           |
|               Output: #[0,1], #[1,0]                                                       |
|               Join Type: INNER                                                             |
|               Join Cond: (#[1,1] = #[0,0])                                                 |
|               ->  Table Scan on tpch.region(cost=0.00..0.00 card=5.00 ndv=0.00 rowsize=0)  |
|                     Output: #[0,0], #[0,1]                                                 |
|                     Table: 'region' (0:'r_regionkey', 1:'r_name')                          |
|               ->  Table Scan on tpch.nation(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0) |
|                     Output: #[0,0], #[0,1]                                                 |
|                     Table: 'nation' (0:'n_name', 1:'n_regionkey')                          |
|                     Filter Cond: (CAST(#[0,1] AS INT64) > 10)                              |
+--------------------------------------------------------------------------------------------+

```

#### Node_INSERT

```sql
mysql> explain verbose INSERT NATION select * from nation;
+---------------------------------------------------------------------------------------------+
| QUERY PLAN                                                                                  |
+---------------------------------------------------------------------------------------------+
| Insert on db1.nation (cost=0.0..0.0 rows=0 ndv=0 rowsize=0)                                 |
|   Output: #[0,0], #[0,1], #[0,2], #[0,3]                                                    |
|   ->  Project(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0)                                |
|         Output: #[0,0], #[0,1], #[0,2], #[0,3]                                              |
|         ->  Table Scan on db1.nation(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0)         |
|               Output: #[0,0], #[0,1], #[0,2], #[0,3]                                        |
|               Table: 'nation' (0:'n_nationkey', 1:'n_name', 2:'n_regionkey', 3:'n_comment') |
+---------------------------------------------------------------------------------------------+
7 rows in set (0.00 sec)
```

#### Node_Update

```sql
mysql> explain verbose UPDATE NATION SET N_NAME ='U1', N_REGIONKEY=2 WHERE N_NATIONKEY > 10 LIMIT 20;
+-------------------------------------------------------------------------------------+
| QUERY PLAN                                                                          |
+-------------------------------------------------------------------------------------+
| Update on db1.nation (cost=0.0..0.0 rows=0 ndv=0 rowsize=0)                         |
|   ->  Project(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0)                        |
|         Output: #[0,0], 'U1', CAST(2 AS INT32)                                      |
|         Limit: 20                                                                   |
|         ->  Table Scan on db1.nation(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0) |
|               Output: #[0,1]                                                        |
|               Table: 'nation' (0:'n_nationkey', 1:'PADDR')                          |
|               Filter Cond: (CAST(#[0,0] AS INT64) > 10)                             |
+-------------------------------------------------------------------------------------+
```

#### Node_Delete

```sql
mysql> explain verbose DELETE FROM NATION WHERE N_NATIONKEY > 10;
+-------------------------------------------------------------------------------------+
| QUERY PLAN                                                                          |
+-------------------------------------------------------------------------------------+
| Delete on db1.nation (cost=0.0..0.0 rows=0 ndv=0 rowsize=0)                         |
|   ->  Project(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0)                        |
|         Output: #[0,0]                                                              |
|         ->  Table Scan on db1.nation(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0) |
|               Output: #[0,1]                                                        |
|               Table: 'nation' (0:'n_nationkey', 1:'PADDR')                          |
|               Filter Cond: (CAST(#[0,0] AS INT64) > 10)                             |
+-------------------------------------------------------------------------------------+
```

With limit:

```sql
mysql>  explain verbose DELETE FROM NATION WHERE N_NATIONKEY > 10 LIMIT 20;
+-------------------------------------------------------------------------------------+
| QUERY PLAN                                                                          |
+-------------------------------------------------------------------------------------+
| Delete on db1.nation (cost=0.0..0.0 rows=0 ndv=0 rowsize=0)                         |
|   ->  Project(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0)                        |
|         Output: #[0,0]                                                              |
|         Limit: 20                                                                   |
|         ->  Table Scan on db1.nation(cost=0.00..0.00 card=25.00 ndv=0.00 rowsize=0) |
|               Output: #[0,1]                                                        |
|               Table: 'nation' (0:'n_nationkey', 1:'PADDR')                          |
|               Filter Cond: (CAST(#[0,0] AS INT64) > 10)                             |
+-------------------------------------------------------------------------------------+
```
