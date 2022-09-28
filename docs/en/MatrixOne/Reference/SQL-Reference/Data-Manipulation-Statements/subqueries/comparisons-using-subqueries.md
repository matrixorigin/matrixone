# **Comparisons Using Subqueries**

## **Description**

The most common use of a subquery is in the form:

```
non_subquery_operand comparison_operator (subquery)
```

Where comparison_operator is one of these operators:

```
=  >  <  >=  <=  <>  !=  <=>
```

## **Syntax**

```
> SELECT column_name(s) FROM table_name WHERE 'a' = (SELECT column1 FROM t1)
```

## **Examples**

```sql
> create table t1 (a int);
> create table t2 (a int, b int);
> create table t3 (a int);
> create table t4 (a int not null, b int not null);
> insert into t1 values (2);
> insert into t2 values (1,7),(2,7);
> insert into t4 values (4,8),(3,8),(5,9);
> insert into t3 values (6),(7),(3);
> select * from t3 where a = (select b from t2);
ERROR 1105 (HY000): scalar subquery returns more than 1 row
> select * from t3 where a = (select distinct b from t2);
+------+
| a    |
+------+
|    7 |
+------+
1 rows in set (0.01 sec)

> select a,b from t4 where a > ( select a ,b from t2 where a>1);
ERROR 1105 (HY000): Internal error: Unknow type TUPLE
```

## **Constraints**

MatrixOne does not support selecting multiple columns for the subquery.
