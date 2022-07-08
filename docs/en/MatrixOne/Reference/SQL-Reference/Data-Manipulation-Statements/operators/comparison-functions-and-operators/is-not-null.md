# **IS NOT NULL**

## **Description**

The `IS NOT NULL` function tests whether a value is not `NULL`.

If expression is `NOT NULL`, this function returns `true`. Otherwise, it returns `false`.

## **Syntax**

```
> expression IS NOT NULL
```

## **Examples**

```sql
> SELECT 1 IS NOT NULL, 0 IS NOT NULL, NULL IS NOT NULL;
+---------------+---------------+------------------+
| 1 is not null | 0 is not null | null is not null |
+---------------+---------------+------------------+
| true          | true          | false            |
+---------------+---------------+------------------+
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
> select * from t1 where b is NOT NULL;
+-------+-------+
| a     | b     |
+-------+-------+
| false | true  |
| true  | false |
| true  | true  |
| false | false |
+-------+-------+
4 rows in set (0.01 sec)
```
