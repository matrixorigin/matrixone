# **IS NULL**

## **语法说明**

`IS NULL` 运算符用于判断列的值是否为空。如果值为空，即为 `NULL`，则返回 `true`，否则返回 `false`。它可以用于 `SELECT`、`INSERT`、`UPDATE` 或 `DELETE` 语句。

## **语法结构**

```
> expression IS NULL
```

## **示例**

```sql
> SELECT 1 IS NULL, 0 IS NULL, NULL IS NULL;
+-----------+-----------+--------------+
| 1 is null | 0 is null | null is null |
+-----------+-----------+--------------+
| false     | false     | true         |
+-----------+-----------+--------------+
1 row in set (0.01 sec)
```

```sql
> create table t1 (a boolean,b bool);
> insert into t1 values (0,1),(true,false),(true,1),(0,false),(NULL,NULL);
> select * from t1;
+-------+-------+
| a     | b     |
+-------+-------+
| false | true  |
| true  | false |
| true  | true  |
| false | false |
| NULL  | NULL  |
+-------+-------+
> select * from t1 where a IS NULL;
+------+------+
| a    | b    |
+------+------+
| NULL | NULL |
+------+------+
1 row in set (0.01 sec)
```
