# **TIMESTAMP()**

## **Description**

With a single argument, this function returns the date or datetime expression expr as a datetime value. With two arguments, it adds the time expression expr2 to the date or datetime expression expr1 and returns the result as a datetime value. Returns `NULL` if expr, expr1, or expr2 is `NULL`.

## **Syntax**

```
> TIMESTAMP(expr), TIMESTAMP(expr1,expr2)
```

## **Arguments**

|  Arguments   | Description  |
|  ----  | ----  |
| expr  | Required.  The expr is an expression specifying the interval value to be added or subtracted from the starting date. The expr is evaluated as a string; it may start with a - for negative intervals. |

## **Examples**

```sql
> SELECT TIMESTAMP('2003-12-31');
+----------------------------+
| timestamp(2003-12-31)      |
+----------------------------+
| 2003-12-31 00:00:00.000000 |
+----------------------------+
1 row in set (0.00 sec)

> CREATE TABLE t1(c1 DATE NOT NULL);
> INSERT INTO t1 VALUES('2000-01-01');
> INSERT INTO t1 VALUES('1999-12-31');
> INSERT INTO t1 VALUES('2000-01-01');
> INSERT INTO t1 VALUES('2006-12-25');
> INSERT INTO t1 VALUES('2008-02-29');
> SELECT TIMESTAMP(c1) FROM t1;
+----------------------------+
| timestamp(c1)              |
+----------------------------+
| 2000-01-01 00:00:00.000000 |
| 1999-12-31 00:00:00.000000 |
| 2000-01-01 00:00:00.000000 |
| 2006-12-25 00:00:00.000000 |
| 2008-02-29 00:00:00.000000 |
+----------------------------+
5 rows in set (0.00 sec)
```

## **Constraints**

`TIMESTAMP()` does not support double arguments for now, which means it doesn't support `TIMESTAMP(expr1,expr2)`.
