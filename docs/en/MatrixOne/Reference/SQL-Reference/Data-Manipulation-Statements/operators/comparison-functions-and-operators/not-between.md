# **NOT BETWEEN ... AND ...**

## **Description**

If expr is less than or equal to min and expr is greater than or equal to max, NOT BETWEEN returns `true`, otherwise it returns `false`.

## **Syntax**

```
> expr NOT BETWEEN min AND max
```

## **Examples**

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
