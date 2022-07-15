# **INSERT INTO SELECT**

## **语法说明**

``INSERT INTO SELECT`` 语句从一个表复制数据，然后把数据插入到一个已存在的表中。且目标表中任何已存在的行都不会受影响。

## **语法结构**

```
INSERT INTO table2 (column1, column2, column3, ...)
SELECT column1, column2, column3, ...
FROM table1
WHERE condition;
```

## **示例**

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
