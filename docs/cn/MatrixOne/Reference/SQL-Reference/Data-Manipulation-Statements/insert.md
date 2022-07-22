# **INSERT**

## **语法描述**

`INSERT`用于在表中插入新行。

## **语法结构**

```
> INSERT INTO [db.]table [(c1, c2, c3)] VALUES (v11, v12, v13), (v21, v22, v23), ...
```

## **示例**

```sql
> CREATE TABLE test(a int, b varchar(10));

> INSERT INTO test values(123, 'abc');

> SELECT * FROM test;
+------+---------+
|   a  |    b    |
+------+---------+
|  123 |   abc   |
+------+---------+
```

## **限制**

目前不支持 `INSERT INTO…VALUES(function)` 语法中的内置函数。
