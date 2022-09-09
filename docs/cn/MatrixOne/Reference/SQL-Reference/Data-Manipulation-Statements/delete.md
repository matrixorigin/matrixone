# **DELETE**

## **语法说明**

`DELETE` 用于删除单表或多表中的记录。

## **语法结构**

### **单表语法结构**

```
DELETE FROM tbl_name [[AS] tbl_alias]
    [WHERE where_condition]
    [ORDER BY ...]
    [LIMIT row_count]
```

`DELETE` 语句从 `tbl_name` 中删除行，并返回已删除的行数。

#### 参数释义

- `WHERE` 从句用于指定用于标识要删除哪些行的条件。若无 `WHERE` 从句，则删除所有行。

- `ORDER BY` 从句，指按照指定的顺序删除行。

- `LIMIT` 从句用于限制可删除的行数。

## **示例**

- **单表示例**

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

- **多表示例**：

同时也支持多表 JOIN 语句。

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
