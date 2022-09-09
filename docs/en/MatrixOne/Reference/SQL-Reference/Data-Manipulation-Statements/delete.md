# **DELETE**

## **Description**

`DELETE` statement removes rows from a single table or multiple tables.

## **Syntax**

### **Single-Table Syntax**

```
DELETE FROM tbl_name [[AS] tbl_alias]
    [WHERE where_condition]
    [ORDER BY ...]
    [LIMIT row_count]
```

The DELETE statement deletes rows from tbl_name and returns the number of deleted rows.

#### Explanations

- The conditions in the optional `WHERE` clause identify which rows to delete. With no `WHERE` clause, all rows are deleted.
- If the `ORDER BY` clause is specified, the rows are deleted in the order that is specified.
-The `LIMIT` clause places a limit on the number of rows that can be deleted.

## **Examples**

- **Single-Table Examples**

```sql
> CREATE TABLE t1 (a bigint(3), b bigint(5) primary key);
> insert INTO t1 VALUES (1,1),(1,2);
> delete from t1 where a=1 limit 1;
> select * from t1;
+------+------+
| a    | b    |
+------+------+
|    1 |    2 |
+------+------+
```

- **Multiple-Table Examples**

Multiple-table join Syntax is also supported.

```sql
> drop table if exists t1;
> drop table if exists t2;
> create table t1 (a int);
> insert into t1 values(1), (2), (4);
> create table t2 (b int);
> insert into t2 values(1), (2), (5);
> delete t1 from t1 join t2 where t1.a = 2;
> select * from t1;
+------+
| a    |
+------+
|    1 |
|    4 |
+------+
2 rows in set (0.00 sec)
```

```sql
> drop database if exists db1;
> drop database if exists db2;
> create database db1;
> create database db2;
> use db2;
> drop table if exists t1;
> create table t1 (a int);
> insert into t1 values (1),(2),(4);
> use db1;
> drop table if exists t2;
> create table t2 (b int);
> insert into t2 values(1),(2),(3);
> delete from db1.t2, db2.t1 using db1.t2 join db2.t1 on db1.t2.b = db2.t1.a where 2 > 1;
> select * from db1.t2;
+------+
| b    |
+------+
|    3 |
+------+
> select * from db2.t1;
+------+
| a    |
+------+
|    4 |
+------+
1 row in set (0.00 sec)
```
