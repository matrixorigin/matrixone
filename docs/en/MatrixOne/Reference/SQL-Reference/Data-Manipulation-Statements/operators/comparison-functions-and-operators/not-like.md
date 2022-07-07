# **NOT LIKE**

## **Description**

The `NOT LIKE` operator is used in a WHERE clause to search for a specified pattern in a column.

There are two wildcards often used in conjunction with the `NOT LIKE` operator:

* The percent sign (%) represents zero, one, or multiple characters.
* The underscore sign (_) represents one, single character.

## **Syntax**

```
> SELECT column1, column2, ...
FROM table_name
WHERE columnN NOT LIKE pattern;
```

## **Examples**

```sql
> create table t1 (a char(10));
> insert into t1 values('abcdef');
> insert into t1 values('_bcdef');
> insert into t1 values('a_cdef');
> insert into t1 values('ab_def');
> insert into t1 values('abc_ef');
> insert into t1 values('abcd_f');
> insert into t1 values('abcde_');
> select * from t1 where a not like 'a%';
+--------+
| a      |
+--------+
| _bcdef |
+--------+
> select * from t1 where a not like "%d_\_";
+--------+
| a      |
+--------+
| abc_ef |
+--------+
1 row in set (0.01 sec)
```
