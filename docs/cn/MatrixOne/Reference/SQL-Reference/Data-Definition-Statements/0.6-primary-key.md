# **PRIMARY KEY**

## **语法说明**

`PRIMARY KEY` 即主键约束，用于唯一标示表中的每条数据。
主键必须包含 `UNIQUE` 值，不能包含 `NULL` 值。
当前版本一个表只能有一个主键，并且这个主键只能有一个列组成。

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
    上述示例中只有一个主键 `PK_Person`, 并且其中仅包含了一列（`ID`）

例如使用如下建表语句时会有错误：

```
> CREATE TABLE Students (
    ID int NOT NULL,
    LastName varchar(255) NOT NULL,
    FirstName varchar(255),
    Age int,
    PRIMARY KEY (ID,LastName)
);
ERROR 1105 (HY000): tae catalog: schema validation: compound idx not supported yet
```

## **限制**

- 目前不支持 带有 `ALTER TABLE` 的 `DROP PRIMARY KEY` 语句。  
