# **SUBQUERY**

A subquery is a SQL query nested inside a larger query.

A subquery may occur in:
- A SELECT clause
- A FROM clause
- A WHERE clause

## Subqueries in the FROM Clause (beta) 

### **Description**

This topic describes subqueries that occur as nested SELECT statements in the FROM clause of an outer SELECT statement. Such subqueries are sometimes called derived tables or table expressions because the outer query uses the results of the subquery as a data source.

### **Syntax**

Every table in a FROM clause must have a name, therefore the [AS] name clause is mandatory. Any columns in the subquery select list must have unique names.

```
> SELECT ... FROM (subquery) [AS] name ...
```

### **Examples**
```sql
> CREATE TABLE tb1 (c1 INT, c2 CHAR(5), c3 FLOAT);
> INSERT INTO tb1 VALUES (1, '1', 1.0);
> INSERT INTO tb1 VALUES (2, '2', 2.0);
> INSERT INTO tb1 VALUES (3, '3', 3.0);
> select * from tb1;
+------+------+--------+
| c1   | c2   | c3     |
+------+------+--------+
|    1 | 1    | 1.0000 |
|    2 | 2    | 2.0000 |
|    3 | 3    | 3.0000 |
+------+------+--------+
3 rows in set (0.03 sec)

> SELECT sc1, sc2, sc3 FROM (SELECT c1 AS sc1, c2 AS sc2, c3*3 AS sc3 FROM tb1) AS sb WHERE sc1 > 1;
+------+------+--------+
| sc1  | sc2  | sc3    |
+------+------+--------+
|    2 | 2    | 6.0000 |
|    3 | 3    | 9.0000 |
+------+------+--------+
2 rows in set (0.02 sec)
```

## Constraints
MatrixOne only support subquery from FROM clause for now.