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

## **Constraints**

Multiple-table Syntax is not supported for now.
