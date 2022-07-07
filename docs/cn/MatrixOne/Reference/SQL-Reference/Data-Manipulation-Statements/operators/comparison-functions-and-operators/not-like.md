# **NOT LIKE**

## **语法说明**

`NOT LIKE` 操作符用于在 `WHERE` 从句中搜索列中的指定模式，是 `LIKE` 的否定用法。

有两个通配符经常与 `LIKE` 操作符一起使用：

* 百分号(%) 代表了0、1或多个字符。
* 下划线(_) 代表单个字符。

## **语法结构**

```
> SELECT column1, column2, ...
FROM table_name
WHERE columnN NOT LIKE pattern;
```

## **示例**

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
