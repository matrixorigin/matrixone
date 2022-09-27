# **DAY()**

## **函数说明**

返回日期，即返回月份的第几号，范围为 1 到 31；对于诸如 `0000-00-00` 或 `2008-00-00` 等包含 0 部分的日期，则返回 0。如果日期为 `NULL` 则返回 `NULL`。

## **函数语法**

```
> DAY(date)
```

## **参数释义**

|  参数   | 说明 |
|  ----  | ----  |
| date| 必要参数。 date 参数是合法的日期表达式。 |

## **示例**

```sql
> select day('2007-02-03');
+-----------------+
| day(2007-02-03) |
+-----------------+
|               3 |
+-----------------+
1 row in set (0.01 sec)

> CREATE TABLE t3(c1 TIMESTAMP NOT NULL);
> INSERT INTO t3 VALUES('2000-01-01');
> INSERT INTO t3 VALUES('1999-12-31');
> INSERT INTO t3 VALUES('2000-01-01');
> INSERT INTO t3 VALUES('2006-12-25');
> INSERT INTO t3 VALUES('2008-02-29');
> select day(c1) from t3;
+---------+
| day(c1) |
+---------+
|       1 |
|      31 |
|       1 |
|      25 |
|      29 |
+---------+
5 rows in set (0.01 sec)
```
