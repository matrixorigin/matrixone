# Obtaining Information with EXPLAIN ANALYZE

`EXPLAIN ANALYZE`, which runs a statement and produces EXPLAIN output along with timing and additional, iterator-based, information about how the optimizer's expectations matched the actual execution. For each iterator, the following information is provided:

- Estimated execution cost

   Some iterators are not accounted for by the cost model, and so are not included in the estimate.

- Estimated number of returned rows

- Time to return first row

- Time spent executing this iterator (including child iterators, but not parent iterators), in milliseconds.

- Number of rows returned by the iterator

- Number of loops

The query execution information is displayed using the TREE output format, in which nodes represent iterators. `EXPLAIN ANALYZE` always uses the TREE output format, also can optionally be specified explicitly using FORMAT=TREE; formats other than TREE remain unsupported.

`EXPLAIN ANALYZE` can be used with `SELECT` statements, as well as with multi-table `UPDATE` and `DELETE` statements.

You can terminate this statement using `KILL QUERY` or `CTRL-C`.

`EXPLAIN ANALYZE` cannot be used with FOR CONNECTION.

## Example

**Create table**

```sql
CREATE TABLE t1 (
    c1 INTEGER DEFAULT NULL,
    c2 INTEGER DEFAULT NULL
);

CREATE TABLE t2 (
    c1 INTEGER DEFAULT NULL,
    c2 INTEGER DEFAULT NULL
);

CREATE TABLE t3 (
    pk INTEGER NOT NULL PRIMARY KEY,
    i INTEGER DEFAULT NULL
);
```

**Example output**:

```sql
> EXPLAIN ANALYZE SELECT * FROM t1 JOIN t2 ON (t1.c1 = t2.c2)\G
*************************** 1. row ***************************
QUERY PLAN: Project
*************************** 2. row ***************************
QUERY PLAN:   Analyze: timeConsumed=0us inputRows=0 outputRows=0 inputSize=0bytes outputSize=0bytes memorySize=0bytes
*************************** 3. row ***************************
QUERY PLAN:   ->  Join
*************************** 4. row ***************************
QUERY PLAN:         Analyze: timeConsumed=5053us inputRows=0 outputRows=0 inputSize=0bytes outputSize=0bytes memorySize=0bytes
*************************** 5. row ***************************
QUERY PLAN:         Join Type: INNER
*************************** 6. row ***************************
QUERY PLAN:         Join Cond: (t1.c1 = t2.c2)
*************************** 7. row ***************************
QUERY PLAN:         ->  Table Scan on aaa.t1
*************************** 8. row ***************************
QUERY PLAN:               Analyze: timeConsumed=2176us inputRows=0 outputRows=0 inputSize=0bytes outputSize=0bytes memorySize=0bytes
*************************** 9. row ***************************
QUERY PLAN:         ->  Table Scan on aaa.t2
*************************** 10. row ***************************
QUERY PLAN:               Analyze: timeConsumed=0us inputRows=0 outputRows=0 inputSize=0bytes outputSize=0bytes memorySize=0bytes
10 rows in set (0.00 sec)

> EXPLAIN ANALYZE SELECT * FROM t3 WHERE i > 8\G
*************************** 1. row ***************************
QUERY PLAN: Project
*************************** 2. row ***************************
QUERY PLAN:   Analyze: timeConsumed=0us inputRows=0 outputRows=0 inputSize=0bytes outputSize=0bytes memorySize=0bytes
*************************** 3. row ***************************
QUERY PLAN:   ->  Table Scan on aaa.t3
*************************** 4. row ***************************
QUERY PLAN:         Analyze: timeConsumed=154us inputRows=0 outputRows=0 inputSize=0bytes outputSize=0bytes memorySize=0bytes
*************************** 5. row ***************************
QUERY PLAN:         Filter Cond: (CAST(t3.i AS BIGINT) > 8)
5 rows in set (0.00 sec)

> EXPLAIN ANALYZE SELECT * FROM t3 WHERE pk > 17\G
*************************** 1. row ***************************
QUERY PLAN: Project
*************************** 2. row ***************************
QUERY PLAN:   Analyze: timeConsumed=0us inputRows=0 outputRows=0 inputSize=0bytes outputSize=0bytes memorySize=0bytes
*************************** 3. row ***************************
QUERY PLAN:   ->  Table Scan on aaa.t3
*************************** 4. row ***************************
QUERY PLAN:         Analyze: timeConsumed=309us inputRows=0 outputRows=0 inputSize=0bytes outputSize=0bytes memorySize=0bytes
*************************** 5. row ***************************
QUERY PLAN:         Filter Cond: (CAST(t3.pk AS BIGINT) > 17)
5 rows in set (0.00 sec)
```

Values shown for actual time in the output of this statement are expressed in milliseconds.
