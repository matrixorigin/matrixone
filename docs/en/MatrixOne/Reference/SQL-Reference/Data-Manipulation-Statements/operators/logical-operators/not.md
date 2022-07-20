# **NOT,!**

## **Description**

Logical `NOT,!`. Evaluates to `true` if the operand is 0, to `false` if the operand is nonzero, and NOT NULL returns NULL.

## **Syntax**

```
> SELECT not column_name FROM table_name;
```

## **Examples**

```sql
> select not 0;
+-------+
| not 0 |
+-------+
| true  |
+-------+
1 row in set (0.02 sec)
> select not null;
+----------+
| not null |
+----------+
| NULL     |
+----------+
1 row in set (0.00 sec)
> select not 1;
+-------+
| not 1 |
+-------+
| false |
+-------+
1 row in set (0.01 sec)
```

```sql
> create table t1 (a boolean,b bool);
> insert into t1 values (0,1),(true,false),(true,1),(0,false),(NULL,NULL);
> select * from t1;
> select not a and not b from t1;
+-----------------+
| not a and not b |
+-----------------+
| false           |
| false           |
| false           |
| true            |
| NULL            |
+-----------------+
5 rows in set (0.00 sec)
```

## **Constraints**

`!` is not supported for now.
