# **DELETE**

## **语法说明**

`DELETE` 用于删除表中的记录。

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

## **限制**

MatrixOne 暂不支持多表删除。
