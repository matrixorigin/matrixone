# **TIMESTAMP()**

## **函数说明**

单个参数时，`TIMESTAMP()` 将日期或日期时间表达式 expr 作为日期时间值返回。两个参数时，`TIMESTAMP()` 将时间表达式 expr2 添加到日期或 datetime 表达式 expr1 中，并将结果作为 datetime 值返回。如果 expr、expr1 或 expr2 为 `NULL`，则返回 `NULL`。

## **函数语法**

```
> TIMESTAMP(expr), TIMESTAMP(expr1,expr2)
```

### **参数释义**

|  参数   | 说明 |
|  ----  | ----  |
| expr  | 必要参数。expr 参数是需要添加进 date 的时间间隔，如果 expr 为负数，那么可以以“-”开头。 |

## **示例**

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

## **限制**

`TIMESTAMP()` 暂不支持双参数，即暂不支持 `TIMESTAMP(expr1,expr2)`。
