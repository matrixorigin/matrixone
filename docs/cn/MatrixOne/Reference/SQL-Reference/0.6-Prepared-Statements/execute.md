# **EXECUTE**

## **语法说明**

`EXECUTE` 语句的作用是：使用 `PREPARE` 准备好一条语句后，可以使用 `EXECUTE` 语句引用预编译的语句名称并执行。如果预编译的语句包含任何参数标记，则必须提供一个 `USING` 子句，该子句列出包含要绑定到参数的值的用户变量。参数值只能由用户变量提供，并且 `USING` 子句必须命名与语句中参数标记的数量一样多的变量。

你可以多次执行给定的预编译语句，将不同的变量传递给它，或者在每次执行之前将变量设置为不同的值。

## **语法结构**

```
EXECUTE stmt_name
    [USING @var_name [, @var_name] ...]
```

## **参数释义**

|  参数   | 说明 |
|  ----  | ----  |
|stmt_name | 预编译的 SQL 语句的名称 |

## **示例**

```sql
> CREATE TABLE numbers(pk INTEGER PRIMARY KEY, ui BIGINT UNSIGNED, si BIGINT);
> INSERT INTO numbers VALUES (0, 0, -9223372036854775808), (1, 18446744073709551615, 9223372036854775807);
> SET @si_min = -9223372036854775808;
> SET @si_max = 9223372036854775807;
> PREPARE s2 FROM 'SELECT * FROM numbers WHERE si=?';
Query OK, 0 rows affected (0.00 sec)

> EXECUTE s2 USING @si_min;
+------+------+----------------------+
| pk   | ui   | si                   |
+------+------+----------------------+
|    0 |    0 | -9223372036854775808 |
+------+------+----------------------+
1 row in set (0.01 sec)

> EXECUTE s2 USING @si_max;
+------+----------------------+---------------------+
| pk   | ui                   | si                  |
+------+----------------------+---------------------+
|    1 | 18446744073709551615 | 9223372036854775807 |
+------+----------------------+---------------------+
1 row in set (0.01 sec)

> DEALLOCATE PREPARE s2;
Query OK, 0 rows affected (0.00 sec)
```
