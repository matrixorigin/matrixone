# Views

This document describes how to use views in MatrixOne.

## Overview

A view acts as a virtual table, whose schema is defined by the `SELECT` statement that creates the view.

**Key Feature**：

- *Simplified user action*: The view mechanism allows users to focus on the data they care about. If the data is not directly from the base table, you can define views to make the database look simple and simplify the user's data query operation.

- *Multiple perspectives on the same data*: The view mechanism enables different users to view the same data differently, which is necessary when many users share the same database.

- *Provides a degree of logical independence for refactoring the database*: Physical data independence means that the user's application does not depend on the physical structure of the database. The logical independence of the data indicates that when the database is restructured, such as adding new relationships or adding new fields to existing relationships, the user's application is not affected. Hierarchical databases and mesh databases can support the physical independence of data but can not fully support logical independence.

## Before you start

- Make sure you have already [installed and launched MatrixOne](https://docs.matrixorigin.io/0.5.1/MatrixOne/Get-Started/install-standalone-matrixone/).
- Use MySQL client to [connect to MatrixOne](https://docs.matrixorigin.io/0.5.1/MatrixOne/Get-Started/connect-to-matrixone-server/).

### Preparation

Create two tables to prepare for using the VIEW:

```sql
> CREATE TABLE t00(a INTEGER);
> INSERT INTO t00 VALUES (1),(2);
> CREATE TABLE t01(a INTEGER);
> INSERT INTO t01 VALUES (1);
```

Query the table *t00*:

```sql
> select * from t00;
+------+
| a    |
+------+
|    1 |
|    2 |
+------+
```

Query the table *t01*:

```sql
> select * from t01;
+------+
| a    |
+------+
|    1 |
+------+
```

## Create a view

A complex query can be defined as a view with the CREATE VIEW statement. The syntax is as follows:

```sql
CREATE VIEW view_name AS query;
```

you cannot create a view with the same name as an existing view or table.

**Example**:

```sql
> CREATE VIEW v0 AS SELECT t00.a, t01.a AS b FROM t00 LEFT JOIN t01 USING(a);
Query OK, 0 rows affected (0.02 sec)
```

## Query views

Once a view is created, you can use the `SELECT` statement to query the view just like a normal table.

```sql
> SELECT * FROM v0;
+------+------+
| a    | b    |
+------+------+
|    1 |    1 |
|    2 | NULL |
+------+------+
```

## Get view related information

Use the `SHOW CREATE TABLE|VIEW view_name` statement:

```sql
> SHOW CREATE VIEW v0;
+------+----------------------------------------------------------------------------+
| View | Create View                                                                |
+------+----------------------------------------------------------------------------+
| v0   | CREATE VIEW v0 AS SELECT t00.a, t01.a AS b FROM t00 LEFT JOIN t01 USING(a) |
+------+----------------------------------------------------------------------------+
1 row in set (0.00 sec)
```

## Drop view

Use the `DROP VIEW view_name;` statement to drop a view.

```sql
DROP VIEW v0;
```
