# **ANY_VALUE**

## **Description**

The `ANY_VALUE` function is useful for `GROUP BY` queries.

## **Syntax**

```
> ANY_VALUE(arg)
```

## **Arguments**

|  Arguments   | Description  |
|  ----  | ----  |
| arg  | Any type. When arg is NULL, the line does not participate in the calculation. |

## **Returned Value**

The function return value and type are the same as the return value and type of its argument.

!!! note  "<font size=4>note</font>"
    <font size=3>The execution result of `ANY_VALUE` is uncertain. The same input may produce different execution results.</font>

## **Examples**

```sql
> create table t1(
    -> a int,
    -> b int,
    -> c int
    -> );
> create table t2(
    -> a int,
    -> b int,
    -> c int
    -> );
> insert into t1 values(1,10,34),(2,20,14);
> insert into t2 values(1,-10,-45);
> select ANY_VALUE(t1.b) from t1 left join t2 on t1.c=t1.b and t1.a=t1.c group by t1.a;
+-----------------+
| any_value(t1.b) |
+-----------------+
|              10 |
|              20 |
+-----------------+
2 rows in set (0.01 sec)
> select 3+(5*ANY_VALUE(t1.b)) from t1 left join t2 on t1.c=t1.b and t1.a=t1.c group by t1.a;
+---------------------------+
| 3 + (5 * any_value(t1.b)) |
+---------------------------+
|                        53 |
|                       103 |
+---------------------------+
2 rows in set (0.00 sec)
```
