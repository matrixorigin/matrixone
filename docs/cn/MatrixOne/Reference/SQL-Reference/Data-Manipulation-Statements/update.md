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

## **限制**

现不支持多表语法。
