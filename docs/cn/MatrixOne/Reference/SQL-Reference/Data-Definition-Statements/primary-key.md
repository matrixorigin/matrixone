# **PRIMARY KEY**

## **语法说明**

`PRIMARY KEY` 即主键约束，用于唯一标示表中的每条数据。
主键必须包含 `UNIQUE` 值，不能包含 `NULL` 值。
一个表只能有一个主键，但一个主键可以包含一个列。

## **在建表时创建主键**

以下SQL语句在创建 `Persons` 表时，在其中的 `ID` 列创建主键：

```
> CREATE TABLE Persons (
    ID int NOT NULL,
    LastName varchar(255) NOT NULL,
    FirstName varchar(255),
    Age int,
    PRIMARY KEY (ID)
);
```

!!! Note 注意区分
    上述示例中只有一个主键 `PK_Person`, 但其中包含了两列（`ID`与`LastName`）

## **限制**

- 目前不支持 带有 `ALTER TABLE` 的 `DROP PRIMARY KEY` 语句。  
- 不支持复合主键（即由多个列组成的主键，为复合主键)
