# **COUNT**

## **Description**

Aggregate function.

The COUNT() function calculates the number of records returned by a select query.

Note: NULL values are not counted.

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

Note: numbers(N) – A table for test with the single number column (UInt64) that contains integers from 0 to N-1.

```
> SELECT count(*) FROM numbers(3);
+----------+
| count(*) |
+----------+
|        3 |
+----------+

> SELECT count(number) FROM numbers(3);
+---------------+
| count(number) |
+---------------+
|             3 |
+---------------+

> SELECT count(number) AS c FROM numbers(3);
+------+
| c    |
+------+
|    3 |
+------+
```

