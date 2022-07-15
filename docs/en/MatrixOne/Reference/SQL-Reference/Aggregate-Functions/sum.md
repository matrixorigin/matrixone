# **SUM**

## **Description**

Aggregate function.

The SUM() function calculates the sum of a set of values.

!!! note  "<font size=4>note</font>"
    <font size=3>NULL values are not counted.</font>  

## **Syntax**

```
> SUM(expr)
```

## **Arguments**

|  Arguments   | Description  |
|  ----  | ----  |
| expr  | Any expression |

## **Returned Value**

Returns the sum of expr. A double if the input type is double, otherwise integer.

If there are no matching rows, SUM() returns NULL.

## **Examples**

```sql
> drop table if exists tbl1,tbl2;
> create table tbl1 (col_1a tinyint, col_1b smallint, col_1c int, col_1d bigint, col_1e char(10) not null);
> insert into tbl1 values (0,1,1,7,"a");
> insert into tbl1 values (0,1,2,8,"b");
> insert into tbl1 values (0,1,3,9,"c");
> insert into tbl1 values (0,1,4,10,"D");
> insert into tbl1 values (0,1,5,11,"a");
> insert into tbl1 values (0,1,6,12,"c");

> select sum(col_1c) from tbl1;
+-------------+
| sum(col_1c) |
+-------------+
|          21 |
+-------------+

> select sum(col_1d) as c1 from tbl1 where col_1d < 13 group by col_1e order by c1;
+------+
| c1   |
+------+
|    8 |
|   10 |
|   18 |
|   21 |
+------+

```

## Constraints

SUM(DISTINCT) is not supported for the 0.5.0 version.
