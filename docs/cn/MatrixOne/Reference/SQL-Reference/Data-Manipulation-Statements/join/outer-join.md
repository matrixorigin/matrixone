# **OUTER JOIN**

## **语法说明**

在 ``OUTER JOIN`` 中，可以返回一个或两个表中的不匹配行。``OUT JOIN``  请参考：

- ``LEFT JOIN`` 关键字从左表（table1）返回所有的行。参见[LEFT JOIN](left-join.md).
- ``RIGHT JOIN`` 关键字从右表（table2）返回所有的行。参见[RIGHT JOIN](right-join.md).
- ``FULL OUTER JOIN`` 关键字只要左表（table1）和右表（table2）其中一个表中存在匹配，则返回行。参见[FULL JOIN](full-join.md).

# **示例**

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
