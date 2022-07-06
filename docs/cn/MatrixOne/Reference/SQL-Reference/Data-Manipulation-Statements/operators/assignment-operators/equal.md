# **=**

## **运算符说明**

`=` 运算符在以下情况下用于赋值：

- 在 `SET` 语句中，`=` 被视为赋值运算符，即运算符 `=` 左侧的用户变量取其右侧的值。`=` 右边的值可以是文本值、另一个存储值的变量，或者任何产生标量值的合法表达式，也可包括查询的结果(前提是这个值是标量值)。可以在同一个 `SET` 语句中执行多个赋值。

- 在 `UPDATE` 语句的 `SET` 子句中，`=` 也充当赋值运算符。你可以在一个 `UPDATE` 语句的同一个 `SET` 子句中进行多个赋值。

- 在其他情况下中，`=` 作为比较运算符。

## **语法结构**

```
> SELECT column1, column2, ...
FROM table_name
WHERE columnN = value1;
```

## **示例**

```sql
> create table t1 (a bigint(3), b bigint(5) primary key);
> insert INTO t1 VALUES (1,1),(1,2);
> update t1 set a=2 where a=1 limit 1;
> select * from t1;
+------+------+
| a    | b    |
+------+------+
|    2 |    1 |
|    1 |    2 |
+------+------+
2 rows in set (0.00 sec)
```
