# **DELETE**

## **Description**

`DELETE` statement removes rows from a table.

## **Syntax**

### **Single-Table Syntax**

```
DELETE [LOW_PRIORITY] [QUICK] [IGNORE] FROM tbl_name [[AS] tbl_alias]
    [PARTITION (partition_name [, partition_name] ...)]
    [WHERE where_condition]
    [ORDER BY ...]
    [LIMIT row_count]
```

The DELETE statement deletes rows from tbl_name and returns the number of deleted rows.

#### Explanations

- The conditions in the optional `WHERE` clause identify which rows to delete. With no `WHERE` clause, all rows are deleted.
- If the `ORDER BY` clause is specified, the rows are deleted in the order that is specified.
-The `LIMIT` clause places a limit on the number of rows that can be deleted.

### **Multiple-Table Syntax**

```
DELETE [LOW_PRIORITY] [QUICK] [IGNORE]
    tbl_name[.*] [, tbl_name[.*]] ...
    FROM table_references
    [WHERE where_condition]
```

Or:

```
DELETE [LOW_PRIORITY] [QUICK] [IGNORE]
    FROM tbl_name[.*] [, tbl_name[.*]] ...
    USING table_references
    [WHERE where_condition]
```

#### Explanations

- The conditions in the optional `WHERE` clause identify which rows to delete. With no `WHERE` clause, all rows are deleted.

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
