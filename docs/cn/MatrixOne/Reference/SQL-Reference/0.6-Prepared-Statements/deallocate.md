# **DEALLOCATE PREPARE**

## **语法说明**

`DEALLOCATE PREPARE` 语句的作用是释放使用 `PREPARE` 生成的预编译语句。在释放预编译语句后，再次执行预编译的语句会导致错误。若创建了过多预编译的语句并且没有使用 `DEALLOCATE PREPARE` 语句进行释放，那么系统变量会强制执行预编译语句上限 `max_prepared_stmt_count` 提示。

## **语法结构**

```
{DEALLOCATE | DROP} PREPARE stmt_name
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
