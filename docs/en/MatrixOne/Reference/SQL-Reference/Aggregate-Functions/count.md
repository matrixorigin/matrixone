# **COUNT**

## **Description**

Aggregate function.

The COUNT() function calculates the number of records returned by a select query.

!!! note  "<font size=4>note</font>"
    <font size=3>NULL values are not counted.</font>  

## **Syntax**

```
> COUNT(expr)
```

***

## **Arguments**

|  Arguments   | Description  |
|  ----  | ----  |
| expr  | Any expression.This may be a column name, the result of another function, or a math operation. * is also allowed, to indicate pure row counting. |

## **Returned Value**

Returns a count of the number of non-NULL values of `expr` in the rows retrieved by a SELECT statement. The result is a BIGINT value.

If there are no matching rows, COUNT() returns 0.

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

> select count(col_1b) from tbl1;
+---------------+
| count(col_1b) |
+---------------+
|             6 |
+---------------+

> select count(*) from tbl1 where col_1d<10;
+----------+
| count(*) |
+----------+
|        3 |
+----------+
```
