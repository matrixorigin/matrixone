# **INSERT**

## **Description**

Writing data.

## **Syntax**

```
> INSERT INTO [db.]table [(c1, c2, c3)] VALUES (v11, v12, v13), (v21, v22, v23), ...
```

## **Examples**

```sql
> CREATE TABLE test(a int, b varchar(10));

> INSERT INTO test values(123, 'abc');

> SELECT * FROM test;
+------+---------+
|   a  |    b    |
+------+---------+
|  123 |   abc   |
+------+---------+

> CREATE TABLE t (i1 INT, d1 DOUBLE, e2 DECIMAL(5,2));
> INSERT INTO t VALUES ( 6, 6.0, 10.0/3), ( null, 9.0, 10.0/3),( 1, null, 10.0/3), ( 2, 2.0, null );
> select * from t;
+------+------+------+
| i1   | d1   | e2   |
+------+------+------+
|    6 |    6 | 3.33 |
| NULL |    9 | 3.33 |
|    1 | NULL | 3.33 |
|    2 |    2 | NULL |
+------+------+------+
4 rows in set (0.00 sec)
```

## **Constraints**

Built-in functions inside `INSERT INTO...VALUES(function)` syntax is not supported yet.
