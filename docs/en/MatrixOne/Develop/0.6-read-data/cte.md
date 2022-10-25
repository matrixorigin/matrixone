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

Download the TPCH test dataset and create the tables. See[TPCH Test](../../Tutorial/TPCH-test-with-matrixone.md).

## CTE Example:

In the following example, `q15_revenue0` is created as a temporary result set and the corresponding query results are cached in MatrixOne. You can perform a formal `q15_revenue0` query better than in a non-CTE scenario.

```sql
with q15_revenue0 as (
    select
        l_suppkey as supplier_no,
        sum(l_extendedprice * (1 - l_discount)) as total_revenue
    from
        lineitem
    where
        l_shipdate >= date '1995-12-01'
        and l_shipdate < date '1995-12-01' + interval '3' month
    group by
        l_suppkey
    )
select
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
from
    supplier,
    q15_revenue0
where
    s_suppkey = supplier_no
    and total_revenue = (
        select
            max(total_revenue)
        from
            q15_revenue0
    )
order by
    s_suppkey
;
```

Result as below:

```
+-----------+--------------------+----------------------------------+-----------------+---------------+
| s_suppkey | s_name             | s_address                        | s_phone         | total_revenue |
+-----------+--------------------+----------------------------------+-----------------+---------------+
|      7895 | Supplier#000007895 | NYl,i8UhxTykLxGJ2voIRn20Ugk1KTzz | 14-559-808-3306 |  1678635.2636 |
+-----------+--------------------+----------------------------------+-----------------+---------------+
```
