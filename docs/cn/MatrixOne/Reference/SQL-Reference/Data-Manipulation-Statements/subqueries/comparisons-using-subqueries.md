# **子查询与比较操作符的使用**

## **语法描述**

子查询与比较操作符最常见的用法如下:

```
non_subquery_operand comparison_operator (subquery)
```

其中，`comparison_operator` 指以下操作符:

```
=  >  <  >=  <=  <>  !=  <=>
```

## **语法结构**

```
> SELECT column_name(s) FROM table_name WHERE 'a' = (SELECT column1 FROM t1)
```

## **示例**

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
+------+
| a    |
+------+
|    7 |
|    7 |
+------+
2 rows in set (0.01 sec)
```
