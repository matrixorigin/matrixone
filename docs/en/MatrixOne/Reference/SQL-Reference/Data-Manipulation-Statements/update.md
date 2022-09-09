# **UPDATE**

## **Description**

The `UPDATE` statement is used to modify the existing records in a table.

## **Syntax**

### **Single-table Syntax**

```
UPDATE table_reference
    SET assignment_list
    [WHERE where_condition]
    [ORDER BY ...]
    [LIMIT row_count]
```

#### Explanations

+ The `UPDATE` statement updates columns of existing rows in the named table with new values.  
+ The `SET` clause indicates which columns to modify and the values they should be given. Each value can be given as an expression, or the keyword `DEFAULT` to set a column explicitly to its default value.
+ The `WHERE` clause, if given, specifies the conditions that identify which rows to update. With no `WHERE` clause, all rows are updated.
+ If the `ORDER BY` clause is specified, the rows are updated in the order that is specified.
+ The `LIMIT` clause places a limit on the number of rows that can be updated.

## **Examples**

- **Single-table Examples**

```sql
> CREATE TABLE t1 (a bigint(3), b bigint(5) primary key);
> insert INTO t1 VALUES (1,1),(1,2);
> update t1 set a=2 where a=1 limit 1;
> select * from t1;
+------+------+
| a    | b    |
+------+------+
|    2 |    1 |
|    1 |    2 |
+------+------+
```

- - **Multiple-table Examples**

```sql
> drop table if exists t1;
> create table t1 (a int);
> insert into t1 values(1), (2), (4);
> drop table if exists t2;
> create table t2 (b int);
> insert into t2 values(1), (2), (3);
> update t1, t2 set a = 1, b =2;
> select * from t1;
+------+
| a    |
+------+
|    1 |
|    1 |
|    1 |
+------+
> update t1, t2 set a = null, b =null;
> select * from t2;
+------+
| b    |
+------+
| NULL |
| NULL |
| NULL |
+------+
> select * from t1;
+------+
| a    |
+------+
| NULL |
| NULL |
| NULL |
+------+
```

Multiple-table join Syntax is also supported.

```sql
> drop table if exists t1;
> drop table if exists t2;
> create table t1 (a int, b int, c int);
> insert into t1 values(1, 2, 3), (4, 5, 6), (7, 8, 9);
> create table t2 (a int, b int, c int);
> insert into t2 values(1, 2, 3), (4, 5, 6), (7, 8, 9);
> update t1 join t2 on t1.a = t2.a set t1.b = 222, t1.c = 333, t2.b = 222, t2.c = 333;
> select * from t1;
+------+------+------+
| a    | b    | c    |
+------+------+------+
|    1 |  222 |  333 |
|    4 |  222 |  333 |
|    7 |  222 |  333 |
+------+------+------+
> with t11 as (select * from (select * from t1) as t22) update t11 join t2 on t11.a = t2.a set t2.b = 666;
> select * from t2;
+------+------+------+
| a    | b    | c    |
+------+------+------+
|    1 |  666 |  333 |
|    4 |  666 |  333 |
|    7 |  666 |  333 |
+------+------+------+
3 rows in set (0.00 sec)
```
