# **<**

## **Description**

The `<` operator returns `true` only if the left-hand operand is less than the right-hand operand.

## **Syntax**

```
> SELECT x < y;
```

## **Examples**

```sql
> SELECT 2 < 2;
+-------+
| 2 < 2 |
+-------+
| false |
+-------+
1 row in set (0.00 sec)
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
> select spID,userID,score from t1 where (userID-1)<spID;
+------+--------+-------+
| spid | userid | score |
+------+--------+-------+
|    1 |      1 |     1 |
|    2 |      2 |     2 |
|    2 |      1 |     4 |
|    3 |      3 |     3 |
|    1 |      1 |     5 |
+------+--------+-------+
5 rows in set (0.00 sec)
> select spID,userID,score from t1 where spID<(userID-1);
+------+--------+-------+
| spid | userid | score |
+------+--------+-------+
|    4 |      6 |    10 |
|    5 |     11 |    99 |
+------+--------+-------+
2 rows in set (0.00 sec)
```
