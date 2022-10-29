# **PREPARE**

## **语法说明**

`PREPARE` 语句准备一条 SQL 语句并给它分配一个名称。

准备好的语句用 `EXECUTE` 执行，用 `DEALLOCATE PREPARE` 释放。

SQL 语句的命名不区分大小写。

## **语法结构**

```
PREPARE stmt_name FROM preparable_stmt
```

## **参数释义**

|  参数   | 说明 |
|  ----  | ----  |
|stmt_name | 预编译的 SQL 语句的名称|
|preparable_stmt|包含 SQL 语句文本的字符串文字或用户变量。文本必须代表单个语句，而不是多个语句。在声明中，*?* 字符可用作参数标记，以指示稍后在执行查询时将数据值绑定到查询的位置。*?* 字符不包含在引号内，即使你打算将它们绑定到字符串值。参数标记只能用于应出现数据值的地方，不能用于 SQL 关键字、标识符等。|

如果已存在具有给定名称的预编译语句，则在准备新的语句之前需要将其隐式释放。

预编译语句创建时需要了解以下几点：

- 在一个会话中创建的预编译语句不可用于其他会话。

- 在一个会话中创建的准备好的语句不适用于其他会话。

- 当会话结束时，无论是正常还是异常，其预编译的语句都不再存在。如果启用了自动重新连接，也不会通知客户端连接丢失。因此，客户端可以禁用自动重新连接。

在预编译的语句中使用的参数需要再首次准备语句时确定其类型，并且在使用 `EXECUTE` 运行预编译语句时保留参数类型。以下内容列出了确定参数类型的规则：

- 二元算术运算符的两个操作数的参数数据类型需一致。

- 如果二元算术运算符的两个操作数都是形式参数，则参数类型由运算符的上下文决定。

- 如果一元算术运算符的操作数是形式参数，则参数类型由运算符的上下文决定。

- 如果算术运算符没有上下文来确定操作数参数类型，则相关参数的的派生类型都是 `DOUBLE PRECISION`。例如，当参数是 `SELECT` 列表中的顶级节点，或者当它是比较运算符的一部分时，那么它相关参数的的派生类型都是 `DOUBLE PRECISION`。

- 字符串运算符的操作数的形式参数具有与其他操作数的聚合类型相同的派生类型。如果运算符的所有操作数都是形式参数，则派生类型为 `VARCHAR`，其排序规则由 `collation_connection` 的值决定 。

- 参数为时间操作符的操作数的形式参数，如果操作符返回 `DATETIME` ，则参数类型为 `DATETIME`；如果操作符返回 `TIME`，则参数类型为 `TIME`；如果操作符返回 `DATE`，则参数类型为 `DATE`。

- 二元比较运算符的操作数的两个参数的派生类型一致。

- 参数为三元比较运算符的操作数的形式参数，例如 `BETWEEN` 运算中的参数，它们的派生类型与其他操作数的聚合类型相同。

- 如果比较运算符的所有操作数都是形式参数，那么每个操作数的派生类型都是 `VARCHAR`，他们的排序规则由 `collation_connection` 的值确定 。

- `CASE`、`COALESCE`、`IF`、`IFNULL` 或 `NULLIF` 中的任意一个输出操作数的形式参数，其派生类型与操作符的其他输出操作数的聚合类型相同。

- 如果 `CASE`、`COALESCE`、`IF`、`IFNULL` 或 `NULLIF` 的所有输出操作数都是形式参数，或者它们都是 `NULL`，则参数的类型由操作符的上下文决定。

- A parameter which is the operand of a CAST() has the same type as specified by the CAST().

- If a parameter is an immediate member of a SELECT list that is not part of an INSERT statement, the derived type of the parameter is VARCHAR, and its collation is determined by the value of collation_connection.

- If a parameter is an immediate member of a SELECT list that is part of an INSERT statement, the derived type of the parameter is the type of the corresponding column into which the parameter is inserted.

- If a parameter is used as source for an assignment in a SET clause of an UPDATE statement or in the ON DUPLICATE KEY UPDATE clause of an INSERT statement, the derived type of the parameter is the type of the corresponding column which is updated by the SET or ON DUPLICATE KEY UPDATE clause.

- If a parameter is an argument of a function, the derived type depends on the function's return type.

— 如果参数是 `CASE`、`COALESCE()`、`IF` 或 `IFNULL` 中的任意一个操作数，并且操作符上下文不能确定其参数类型，则每个参数的派生类型都是 `VARCHAR`，其排序规则由 `collation_connection` 的值决定。

— `CAST()` 操作数的形式参数与 `CAST()` 指定的类型相同。

— 如果一个形式参数是 `SELECT` 列表的直接成员，而不是 `INSERT` 语句的一部分，则形式参数的派生类型为 `VARCHAR`，其排序规则由 `collation_connection` 的值决定。

— 如果形式参数是 `INSERT` 语句的一部分 `SELECT` 列表的直接成员，则形式参数的派生类型为插入形式参数的对应列的类型。

- 如果一个形式参数被用作 `UPDATE` 语句的 `SET` 子句或 `INSERT` 语句的 `ON DUPLICATE KEY UPDATE` 子句中的赋值源，形式参数的派生类型是由 `SET` 或 `ON DUPLICATE KEY UPDATE` 子句更新的对应列的类型。

- 如果形式参数是函数的实际参数，则其派生类型取决于函数的返回类型。

对于实际类型和派生类型的某些组合，会触发自动重新准备预编译语句。以下情况，无需重新准备预编译语句：

- `NULL` 为实际参数。

- 形式参数是 `CAST()` 的操作数。(`CAST()` 会尝试将参数转换到派生类型，如果转换失败则会引发异常。)

- 参数类型为字符串。(在本例中，隐式执行 `CAST(? AS derived_type)`)。

- 参数的派生类型和实际类型均为 `INTEGER`，且具有相同的符号。

- 参数的派生类型为 `DECIMAL`，实际类型为 `DECIMAL` 或 `INTEGER`。

- 参数的派生类型为 `DOUBLE`，实际类型为任意数字类型。

- 参数的派生类型和实际类型都是字符串类型。

- 参数派生类型是时态类型，实际类型是时态类型。异常情况：参数的派生类型是 `TIME`，而实际类型不是 `TIME`；参数的派生类型是 `DATE`，而实际类型不是 `DATE`。

- 派生类型是时间类型，实际类型是数字类型。

除上述情况以外，需要重新准备预编译语句并使用实际参数类型，不能使用派生参数类型。

这些规则也适用于在预编译语句中引用的用户变量。

在预编译语句中为给定参数或用户变量时，如果在第一次执行时使用不同的数据类型，则会导致重新准备该预编译语句，不但运行效率低，而且还可能导致参数（或变量）的实际类型发生变化，实际执行结果与语句的预期结果不一致。建议在预编译语句中对给定参数使用相同的数据类型。

## **示例**

```sql
> create table t13 (a int primary key);
> insert into t13 values (1);
> select * from t13 where 3 in (select (1+1) union select 1);
Empty set (0.01 sec)

> select * from t13 where 3 in (select (1+2) union select 1);
+------+
| a    |
+------+
|    1 |
+------+
1 row in set (0.01 sec)

> prepare st_18492 from 'select * from t13 where 3 in (select (1+1) union select 1)';
Query OK, 0 rows affected (0.00 sec)

> execute st_18492;
Empty set (0.01 sec)

> prepare st_18493 from 'select * from t13 where 3 in (select (2+1) union select 1)';
Query OK, 0 rows affected (0.00 sec)

> execute st_18493;
+------+
| a    |
+------+
|    1 |
+------+
1 row in set (0.00 sec)

> deallocate prepare st_18492;
Query OK, 0 rows affected (0.00 sec)

> deallocate prepare st_18493;
Query OK, 0 rows affected (0.00 sec)
```
