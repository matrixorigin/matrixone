# MatrixOne Query Execution Plan Overview

The `EXPLAIN` statement shows the MatrixOne execution plan for the SQL statement.

!!! note
    When you use the MySQL client to connect to MatrixOne, to read the output result in a clearer way without line wrapping, you can use the `pager less -S` command. Then, after the `EXPLAIN` result is output, you can press the right arrow **→** button on your keyboard to horizontally scroll through the output.

## EXPLAIN Example

**Data preparation**：

```sql
CREATE TABLE t (id INT NOT NULL PRIMARY KEY auto_increment, a INT NOT NULL, pad1 VARCHAR(255), INDEX(a));
INSERT INTO t VALUES (1, 1, 'aaa'),(2,2, 'bbb');
EXPLAIN SELECT * FROM t WHERE a = 1;
```

**Return result**：

```sql
+------------------------------------------------+
| QUERY PLAN                                     |
+------------------------------------------------+
| Project                                        |
|   ->  Table Scan on aab.t                      |
|         Filter Cond: (CAST(t.a AS BIGINT) = 1) |
+------------------------------------------------+
```

`EXPLAIN` does not execute the actual query. `EXPLAIN ANALYZE` can be used to execute the query and show `EXPLAIN` information. This can be useful in diagnosing cases where the execution plan selected is suboptimal.

**EXPLAIN output analysis**

- QUERY PLAN: the name of an operator.

   + Filter Cond：Filter conditions
   + Table Scan：scans the table

- **Project** is the parent node of the executive order in the query process. The structure of the Project is tree-like, and the child node "flows into" the parent node after the calculation is completed. The parent, child, and sibling nodes may execute parts of the query in parallel.

**Range query**

In the `WHERE/HAVING/ON` conditions, the MatrixOne optimizer analyzes the result returned by the primary key query. For example, these conditions might include comparison operators of the numeric and date type, such as `>`, `<`, `=`, `>=`, `<=`, and the character type such as `LIKE`.
