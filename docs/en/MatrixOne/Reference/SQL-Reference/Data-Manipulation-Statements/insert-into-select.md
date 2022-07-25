# **INSERT INTO SELECT**

## **Description**

The ``INSERT INTO SELECT`` statement copies data from one table and inserts it into another table.

The existing records in the target table are unaffected.

## **Syntax**

```
INSERT INTO table2 (column1, column2, column3, ...)
SELECT column1, column2, column3, ...
FROM table1
WHERE condition;
```

## **Examples**

```sql
> create table t1(id int, name varchar(10));
> insert into t1 values(1, 'a');
> insert into t1 values(2, 'b');
> insert into t1 values(3, 'c');
> create table t2(id int, appname varchar(10), country varchar(10));
> insert into t2 values(1, 'appone', 'CN');
> insert into t2 values(2, 'apptwo', 'CN');
> INSERT INTO t1 (name) SELECT appname FROM t2;
> Query OK, 2 rows affected (0.01 sec)
> select * from t1;
+------+--------+
| id   | name   |
+------+--------+
|    1 | a      |
|    2 | b      |
|    3 | c      |
| NULL | appone |
| NULL | apptwo |
+------+--------+
```
