# **CREATE TABLE**

## **语法说明**
`CREATE TABLE` 语句用于在当前所选数据库创建一张新表。

## **语法结构**

```
> CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    name1 type1,
    name2 type2,
    ...
)
```

#### 语法图:

![Create Table Diagram](https://github.com/matrixorigin/artwork/blob/main/docs/reference/create_table_statement.png?raw=true)

## **示例**
```
> CREATE TABLE test(a int, b varchar(10));

> INSERT INTO test values(123, 'abc');

> SELECT * FROM test;
+------+---------+
|   a  |    b    |
+------+---------+
|  123 |   abc   |
+------+---------+
```