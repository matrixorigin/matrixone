# **SUM**

## **Description**

Aggregate function.

The SUM() function calculates the sum of a set of values.

Note: NULL values are not counted.

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

f there are no matching rows, SUM() returns NULL.

## **Examples**

Note: numbers(N) – A table for test with the single number column (UInt64) that contains integers from 0 to N-1.

```
> SELECT SUM(*) FROM numbers(3);
+--------+
| sum(*) |
+--------+
|      3 |
+--------+

> SELECT SUM(number) FROM numbers(3);
+-------------+
| sum(number) |
+-------------+
|           3 |
+-------------+

> SELECT SUM(number) AS sum FROM numbers(3);
+------+
| sum  |
+------+
|    3 |
+------+

> SELECT SUM(number+2) AS sum FROM numbers(3);
+------+
| sum  |
+------+
|    9 |
+------+
```

***
