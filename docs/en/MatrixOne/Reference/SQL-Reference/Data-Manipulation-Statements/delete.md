# **DELETE**

## **Description**

`DELETE` statement removes rows from a table.

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

## **Constraints**

MatrixOne doesn't support deleting multiple-table.
