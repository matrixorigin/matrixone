# **MAX**

## **Description**

Aggregate function.

The MAX() function calculates the maximum value across a group of values.

## **Syntax**

```
$ MAX(expr)
```

## **Arguments**
|  Arguments   | Description  |
|  ----  | ----  |
| expr  | Any expression |

## **Returned Value**
Returns the maximum value of expr. MAX() may take a string argument, in such cases, it returns the maximum string value. 

## **Examples**

Note: numbers(N) – A table for test with the single number column (UInt64) that contains integers from 0 to N-1.

```
$ SELECT MAX(*) FROM numbers(3);
+--------+
| max(*) |
+--------+
|      2 |
+--------+

$ SELECT MAX(number) FROM numbers(3);
+-------------+
| max(number) |
+-------------+
|           2 |
+-------------+

$ SELECT MAX(number) AS max FROM numbers(3);
+------+
| max  |
+------+
|    2 |
+------+
```


***