# **IF**

## **Description**

The `IF()` function returns a value if a condition is `TRUE`, or another value if a condition is `FALSE`.

## **Syntax**

```
> IF(expr1,expr2,expr3)
```

- If expr1 is TRUE (expr1 <> 0 and expr1 IS NOT NULL), IF() returns expr2. Otherwise, it returns expr3.

- If only one of expr2 or expr3 is explicitly NULL, the result type of the IF() function is the type of the non-NULL expression.

- The default return type of IF() (which may matter when it is stored into a temporary table) is calculated as follows:

  + If expr2 or expr3 produce a string, the result is a string.

  + If expr2 and expr3 are both strings, the result is case-sensitive if either string is case-sensitive.

  + If expr2 or expr3 produce a floating-point value, the result is a floating-point value.

  + If expr2 or expr3 produce an integer, the result is an integer.

## **Examples**

```sql
> SELECT IF(1>2,2,3);
+-----------------+
| if(1 > 2, 2, 3) |
+-----------------+
|               3 |
+-----------------+
1 row in set (0.01 sec)
> SELECT IF(1<2,'yes','no');
+--------------------+
| if(1 < 2, yes, no) |
+--------------------+
| yes                |
+--------------------+
1 row in set (0.00 sec)
```

```sql
> CREATE TABLE t1 (st varchar(255) NOT NULL, u int(11) NOT NULL);
> INSERT INTO t1 VALUES ('a',1),('A',1),('aa',1),('AA',1),('a',1),('aaa',0),('BBB',0);
> select if(u=1,st,st) s from t1 order by s;
+------+
| s    |
+------+
| A    |
| AA   |
| BBB  |
| a    |
| a    |
| aa   |
| aaa  |
+------+
7 rows in set (0.00 sec)

> select if(u=1,st,st) s from t1 where st like "%a%" order by s;
+------+
| s    |
+------+
| a    |
| a    |
| aa   |
| aaa  |
+------+
4 rows in set (0.00 sec)
```

## **Constraints**

Parameters BIGINT and VARCHAR are not supported with the function 'if'.
