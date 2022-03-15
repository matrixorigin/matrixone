# **CREATE INDEX**

## **语法说明**
`CREATE INDEX` 语句为已有表`table_name`的某列添加新索引。

## **语法结构**

```
> CREATE INDEX index_name ON table_name (column_name)

```


## **示例**
```
> CREATE INDEX PIndex ON Persons (LastName);

```

## **限制**

目前索引只能应用于单列。此外，还不支持指定索引类型，`UNIQUE/FULLTEXT` 语句还不支持。