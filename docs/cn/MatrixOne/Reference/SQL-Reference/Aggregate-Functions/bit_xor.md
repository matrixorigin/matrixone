# **BIT_XOR**

## **函数说明**

BIT_XOR()是一个聚合函数，计算了列中所有位的按位异或。

## **函数语法**

```
> BIT_XOR(expr)
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

> select id, bit_xor(number) from t1 group by id;
+------+-----------------+
| id   | bit_xor(number) |
+------+-----------------+
| a    |             101 |
| b    |              10 |
+------+-----------------+
```
