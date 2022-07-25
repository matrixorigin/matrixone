# **MAX**

## **Description**

Aggregate function.

The MAX() function calculates the maximum value across a group of values.

## **Syntax**

```
> MAX(expr)
```

## **Arguments**

|  Arguments   | Description  |
|  ----  | ----  |
| expr  | Any expression |

## **Returned Value**

Returns the maximum value of expr. MAX() may take a string argument, in such cases, it returns the maximum string value.

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

> select max(col_1d) from tbl1;
+-------------+
| max(col_1d) |
+-------------+
|          12 |
+-------------+

> select max(col_1c) as m1 from tbl1 where col_1d<12 group by col_1e;
+------+
| m1   |
+------+
|    5 |
|    2 |
|    3 |
|    4 |
+------+
```
