# **DELETE**

## **语法说明**

`DELETE` 用于删除表中的记录。

## **语法结构**

### **单表语法结构**

```
DELETE FROM tbl_name [[AS] tbl_alias]
    [PARTITION (partition_name [, partition_name] ...)]
    [WHERE where_condition]
    [ORDER BY ...]
    [LIMIT row_count]
```

`DELETE` 语句从 `tbl_name` 中删除行，并返回已删除的行数。

#### 参数释义

- `WHERE` 从句用于指定用于标识要删除哪些行的条件。若无 `WHERE` 从句，则删除所有行。

- `ORDER BY` 从句，指按照指定的顺序删除行。

- `LIMIT` 从句用于限制可删除的行数。

### **多表语法结构**

```
DELETE [LOW_PRIORITY] [QUICK] [IGNORE]
    tbl_name[.*] [, tbl_name[.*]] ...
    FROM table_references
    [WHERE where_condition]
```

```
DELETE [LOW_PRIORITY] [QUICK] [IGNORE]
    FROM tbl_name[.*] [, tbl_name[.*]] ...
    USING table_references
    [WHERE where_condition]
```

#### 参数释义

- `WHERE` 从句用于指定用于标识要删除哪些行的条件。若无 `WHERE` 从句，则删除所有行。

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

- **多表示例**

```sql
> create table t11 (a bigint NOT NULL, b int, primary key (a));
> create table t12 (a bigint NOT NULL, b int, primary key (a));
> create table t2 (a bigint NOT NULL, b int, primary key (a));
> insert into t11 values (0, 10),(1, 11),(2, 12);
> insert into t12 values (33, 10),(0, 11),(2, 12);
> insert into t2 values (0, 21),(1, 12),(3, 23);
> delete from t11 where t11.b <> (select b from t2 where t11.a = t2.a);
> select * from t11;
+------+------+
| a    | b    |
+------+------+
|    2 |   12 |
+------+------+
```
