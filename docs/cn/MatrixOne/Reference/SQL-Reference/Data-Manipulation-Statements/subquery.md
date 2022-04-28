# **子查询**

子查询，也称为嵌套查询或子选择，是SELECT嵌入在另一个SQL查询的查询方式。

一个子查询有几类：
- SELECT子查询
- FROM子查询
- WHERE子查询

## FROM子查询 (beta) 

### **描述**

当SELECT语句的FROM子句中使用独立子查询时，我们也经常将其称为派生表，因为实际上外部查询将子查询的结果当作了一个数据源。

### **语法结构**

每个FROM子查询的表都必须要有一个名字，因此[AS]操作符是必须的。子查询的SELECT列表中每个列也必须要有一个唯一的名字。

```
> SELECT ... FROM (subquery) [AS] name ...
```

### **示例**
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

## **限制**
MatrixOne目前仅支持FROM子查询。