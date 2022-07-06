# **=**

## **Description**

The `=` operator returns `true` only if the left-hand operand is equal to the right-hand operand.

## **Syntax**

```
> SELECT x = y;
```

## **Examples**

```sql
> SELECT 2 = 2;
+-------+
| 2 = 2 |
+-------+
| true  |
+-------+
1 row in set (0.01 sec)
```

```sql
> create table t1 (spID smallint,userID bigint,score int);
> insert into t1 values (1,1,1);
> insert into t1 values (2,2,2);
> insert into t1 values (2,1,4);
> insert into t1 values (3,3,3);
> insert into t1 values (1,1,5);
> insert into t1 values (4,6,10);
> insert into t1 values (5,11,99);
> select userID,spID,score from t1 where userID=spID and userID=score;
+--------+------+-------+
| userid | spid | score |
+--------+------+-------+
|      1 |    1 |     1 |
|      2 |    2 |     2 |
|      3 |    3 |     3 |
+--------+------+-------+
3 rows in set (0.00 sec)
```
