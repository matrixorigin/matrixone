# **BIT_AND**

## **函数说明**

BIT_AND()是一个聚合函数，计算了列中所有位的按位与。

## **函数语法**

```
> BIT_AND(expr)
```

## **参数释义**

|  参数   | 说明 |
|  ----  | ----  |
| expr  | UINT类型的列|

## **示例**

```sql
> drop table if exists t1;
> CREATE TABLE t1 (id CHAR(1), number INT);
> INSERT INTO t1 VALUES
      ('a',111),('a',110),('a',100),
      ('a',000),('b',001),('b',011);

> select id, BIT_AND(number) FROM t1 GROUP BY id;
+------+-----------------+
| id   | bit_and(number) |
+------+-----------------+
| a    |               0 |
| b    |               1 |
+------+-----------------+
```
