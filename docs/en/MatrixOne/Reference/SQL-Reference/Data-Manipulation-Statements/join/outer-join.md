# **OUTER JOIN**

## **Description**

When performing an ``INNER JOIN``, rows from either table that are unmatched in the other table are not returned. In an ``OUTER JOIN``, unmatched rows in one or both tables can be returned. There are a few types of outer joins:

- ``LEFT JOIN`` returns only unmatched rows from the left table. For more information, see [LEFT JOIN](left-join.md).
- ``RIGHT JOIN`` returns only unmatched rows from the right table.For more information, see [RIGHT JOIN](right-join.md).
- ``FULL OUTER JOIN`` returns unmatched rows from both tables.For more information, see [FULL JOIN](full-join.md).

# **Examples**

```sql
create table t1 (a1 int, a2 char(3));
insert into t1 values(10,'aaa'), (10,null), (10,'bbb'), (20,'zzz');
create table t2(a1 char(3), a2 int, a3 real);
insert into t2 values('AAA', 10, 0.5);
insert into t2 values('BBB', 20, 1.0);
select t1.a1, t1.a2, t2.a1, t2.a2 from t1 left outer join t2 on t1.a1=10;
+------+------+------+------+
| a1   | a2   | a1   | a2   |
+------+------+------+------+
|   10 | aaa  | AAA  |   10 |
|   10 | aaa  | BBB  |   20 |
|   10 | NULL | AAA  |   10 |
|   10 | NULL | BBB  |   20 |
|   10 | bbb  | AAA  |   10 |
|   10 | bbb  | BBB  |   20 |
|   20 | zzz  | NULL | NULL |
+------+------+------+------+
```
