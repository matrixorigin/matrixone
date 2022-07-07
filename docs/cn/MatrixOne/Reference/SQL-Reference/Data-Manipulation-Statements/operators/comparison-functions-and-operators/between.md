# **BETWEEN ... AND ...**

## **语法说明**

`BETWEEN ... AND ...` 操作符选取介于两个值之间的数据范围内的值。这些值可以是数值、文本或者日期。如果取值介于两值之间，那么返回 `true`，否则返回结果为 `false`。

## **语法结构**

```
> expr BETWEEN min AND max
```

## **示例**

```sql
> SELECT 2 BETWEEN 1 AND 3, 2 BETWEEN 3 and 1;
+-------------------+-------------------+
| 2 between 1 and 3 | 2 between 3 and 1 |
+-------------------+-------------------+
| true              | false             |
+-------------------+-------------------+
1 row in set (0.01 sec)
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
> select count(*) from t where id between 8894754949779693574 and 17790886498483827171;
+----------+
| count(*) |
+----------+
|        0 |
+----------+
1 row in set (0.01 sec)
```
