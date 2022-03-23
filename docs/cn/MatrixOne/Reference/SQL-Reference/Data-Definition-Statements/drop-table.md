# **DROP TABLE**

## **语法说明**
该语句用于从当前所选的数据库中删除表，如果表不存在则会报错，除非使用 `IF EXISTS` 修饰符。

## **语法结构**

```
> DROP TABLE [IF EXISTS] [db.]name
```

#### 语法图:

![Drop Table Diagram](https://github.com/matrixorigin/artwork/blob/main/docs/reference/drop_table_statement.png?raw=true)

## **示例**
```
> CREATE TABLE table01(a int);
> DROP TABLE table01;
```
