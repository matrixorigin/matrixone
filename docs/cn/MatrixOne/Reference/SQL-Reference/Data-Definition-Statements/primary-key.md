# **PRIMARY KEY**

## **语法说明**
`PRIMARY KEY` 即主键约束，用于唯一标示表中的每条数据。
主键必须包含 `UNIQUE` 值，不能包含 `NULL` 值。
一个表只能有一个主键，但一个主键可以包含一个或多个列。



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


若要对主键进行命名，并指定多个列为主键，可参考如下语句：  
```
> CREATE TABLE Persons (
    ID int NOT NULL,
    LastName varchar(255) NOT NULL,
    FirstName varchar(255),
    Age int,
    CONSTRAINT PK_Person PRIMARY KEY (ID,LastName)
);
```

!!! Note 注意区分
    上述示例中只有一个主键 `PK_Person`, 但其中包含了两列（`ID `与`LastName`）


## **限制**

目前不支持 `DROP PRIMARY KEY` 语句。