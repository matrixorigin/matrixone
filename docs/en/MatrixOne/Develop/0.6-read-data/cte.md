# Common Table Expression

A CTE (Common table expression) is a named temporary result set that exists only within the execution scope of a single SQL statement (such as `SELECT`, `INSERT`, `UPDATE`, or `DELETE`).

Like derived tables, CTEs are not stored as objects and persist only for the duration of query execution; Unlike derived tables, CTEs can be self-referenced (recursive Ctes, not currently supported) or referenced multiple times in the same query. In addition, CTEs provide better readability and performance than derived tables.

The structure of a CTE includes a name, a list of optional columns, and the query that defines the CTE.

CTE Statement:

```sql
WITH <query_name> AS (
    <query_definition>
)
SELECT ... FROM <query_name>;
```

## Before you start

- Make sure you have already [installed and launched MatrixOne](https://docs.matrixorigin.io/0.5.1/MatrixOne/Get-Started/install-standalone-matrixone/).
- Use MySQL client to [connect to MatrixOne](https://docs.matrixorigin.io/0.5.1/MatrixOne/Get-Started/connect-to-matrixone-server/).

### Preparation

You can create a simple table and insert some data to help you understand the CTE statements shown later: 

```sql
> drop table if exists t1;
> create table t1(a int, b int, c int);
> insert into t1 values(null,null,null),(2,3,4);
```

## CTE Example

In the following example, `qn` is created as a temporary result set and the corresponding query results are cached in MatrixOne. You can perform a formal `qn` query better than in a non-CTE scenario.

```sql
WITH qn AS (SELECT a FROM t1), qn2 as (select b from t1)
SELECT * FROM qn;
```

Result as below:

```
+------+
| a    |
+------+
| NULL |
|    2 |
+------+
```
