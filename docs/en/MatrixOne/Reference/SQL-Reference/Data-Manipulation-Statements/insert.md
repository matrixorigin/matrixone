# **INSERT**

## **Description**
Writing data.

## **Syntax**

```
$ INSERT INTO [db.]table [(c1, c2, c3)] VALUES (v11, v12, v13), (v21, v22, v23), ...
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
