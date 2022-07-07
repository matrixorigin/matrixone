# **IF**

## **语法说明**

`IF()` 既可以作为表达式用，也可在存储过程中作为流程控制语句使用。

## **语法结构**

```
> IF(expr1,expr2,expr3)
```

- 如果 expr1 为 `TRUE` (expr1 <> 0 and expr1 IS NOT NULL)，则结果返回 expr2；否则，返回 expr3。

— 如果 expr2 或 expr3 中只有一个显式为 `NULL`，则 `If()` 函数的结果类型为非 NULL 表达式的类型。

- `IF()`的默认返回类型(存储到临时表时可能会有影响)计算如下：

  + 如果 expr2 或 expr3 生成的是字符串，则结果为字符串。

  + 如果 expr2 和 expr3 都是字符串，如果字符串中有一个是区分大小写的，则结果也是区分大小写的。

  + 如果 expr2 或 expr3 生成的是浮点值，则结果为浮点值。

## **示例**

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

## **限制**

Parameters BIGINT and VARCHAR are not supported with the function 'if'.
