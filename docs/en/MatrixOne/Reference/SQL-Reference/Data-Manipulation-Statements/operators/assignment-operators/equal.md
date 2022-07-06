# **=**

## **Description**

This `=` operator is used to perform value assignments in the below cases:

- Within a `SET` statement, `=` is treated as an assignment operator that causes the user variable on the left hand side of the operator to take on the value to its right. The value on the right hand side may be a literal value, another variable storing a value, or any legal expression that yields a scalar value, including the result of a query (provided that this value is a scalar value). You can perform multiple assignments in the same SET statement.

- In the `SET` clause of an `UPDATE` statement, `=` also acts as an assignment operator. You can make multiple assignments in the same `SET` clause of an `UPDATE` statement.

- In any other context, `=` is treated as a comparison operator.

## **Syntax**

```
> SELECT column1, column2, ...
FROM table_name
WHERE columnN = value1;
```

## **Examples**

```sql
> create table t1 (a bigint(3), b bigint(5) primary key);
> insert INTO t1 VALUES (1,1),(1,2);
> update t1 set a=2 where a=1 limit 1;
> select * from t1;
+------+------+
| a    | b    |
+------+------+
|    2 |    1 |
|    1 |    2 |
+------+------+
2 rows in set (0.00 sec)
```
