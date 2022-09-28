# **UPDATE**

## **语法描述**

`UPDATE` 用于修改表中的现有记录。

## **语法结构**

### **单表语法结构**

```
UPDATE table_reference
    SET assignment_list
    [WHERE where_condition]
    [ORDER BY ...]
    [LIMIT row_count]
```

#### 参数释义

- `UPDATE` 将新值更新到指定表中现有行的列中。
- `SET` 从句指出要修改哪些列以及它们应该被赋予的值。每个值可以作为表达式给出，或者通过 `DEFAULT` 明确将列设置为默认值。
- `WHERE` 从句，用于指定用于标识要更新哪些行的条件。若无 `WHERE` 从句，则更新所有行。
- `ORDER BY` 从句，指按照指定的顺序更新行。
- `LIMIT` 从句用于限制可更新的行数。

## **示例**

- **单表示例**

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

- **多表示例**

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

支持多表 JOIN 语句。

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
