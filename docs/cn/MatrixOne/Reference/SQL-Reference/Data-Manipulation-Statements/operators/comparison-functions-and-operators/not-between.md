# **NOT BETWEEN ... AND ...**

## **语法说明**

`NOT BETWEEN ... AND ...` 操作符选取介于两个值之间的数据范围外的值。这些值可以是数值、文本或者日期。如果取值介于两值之间，那么返回 `false`，否则返回结果为 `true`。

## **语法结构**

```
> expr NOT BETWEEN min AND max
```

## **示例**

```sql
> SELECT 2 NOT BETWEEN 1 AND 3, 2 NOT BETWEEN 3 and 1;
+-----------------------+-----------------------+
| 2 not between 1 and 3 | 2 not between 3 and 1 |
+-----------------------+-----------------------+
| false                 | true                  |
+-----------------------+-----------------------+
1 row in set (0.00 sec)
```

```sql
> create table t (id bigint unsigned, b int);
> insert into t values(8894754949779693574,1);
> insert into t values(8894754949779693579,2);
> insert into t values(17790886498483827171,3);
> select count(*) from t where id>=8894754949779693574 and id =17790886498483827171 order by 1 asc;
+----------+
| count(*) |
+----------+
|        0 |
+----------+
> select count(*) from t where id not between 8894754949779693574 and 17790886498483827171;
+----------+
| count(*) |
+----------+
|        3 |
+----------+
1 row in set (0.00 sec)
```
