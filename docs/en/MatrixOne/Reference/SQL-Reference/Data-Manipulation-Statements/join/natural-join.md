# **NATURAL JOIN**

## **Description**

The ``NATURAL JOIN`` of two tables is defined to be semantically equivalent to an ``INNER JOIN`` or a ``LEFT JOIN`` with a ``USING`` clause that names all columns that exist in both tables.

## **Syntax**

```
> SELECT table_column1, table_column2...
FROM table_name1
NATURAL JOIN table_name2;

```

## **Examples**

```sql
> create table t1(id int,desc1 varchar(50),desc2 varchar(50));
> create table t2(id int,desc3 varchar(50),desc4 varchar(50));
> INSERT INTO t1(id,desc1,desc2) VALUES(100,'desc11','desc12'),(101,'desc21','desc22'),(102,'desc31','desc32');
> INSERT INTO t2(id,desc3,desc4) VALUES(101,'desc41','desc42'),(103,'desc51','desc52'),(105,'desc61','desc62');
> SELECT t1.id,t2.id,desc1,desc2,desc3,desc4 FROM t1 NATURAL JOIN t2;
+------+------+--------+--------+--------+--------+
| id   | id   | desc1  | desc2  | desc3  | desc4  |
+------+------+--------+--------+--------+--------+
|  101 |  101 | desc21 | desc22 | desc41 | desc42 |
+------+------+--------+--------+--------+--------+
1 row in set (0.00 sec)

```
