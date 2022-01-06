# **CREATE INDEX**

## **Description**
Create an index on a table column.

## **Syntax**

```
> CREATE INDEX index_name ON table_name (column_name)

```


## **Examples**
```
> CREATE INDEX PIndex ON Persons (LastName);

```

## **Constraints**

The index can only be applied for a single column. The index type, UNIQUE/FULLTEXT statements are not supported yet.