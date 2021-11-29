# **CREATE TABLE**

## **Description**
Create a new table.

## **Syntax**

```
$ CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    name1 type1,
    name2 type2,
    ...
)
```

## **Examples**
```
$ CREATE TABLE test(a int, b varchar(10));

$ INSERT INTO test values(123, 'abc');

$ SELECT * FROM test;
+------+---------+
|   a  |    b    |
+------+---------+
|  123 |   abc   |
+------+---------+
```