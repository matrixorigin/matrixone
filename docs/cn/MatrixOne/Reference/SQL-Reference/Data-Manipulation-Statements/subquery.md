# **子查询**

子查询，也称为嵌套查询或子选择，是 `SELECT` 子查询语句嵌入在另一个 `SQL` 查询的查询方式。

参见下面的子查询示例：

```
SELECT * FROM t1 WHERE column1 = (SELECT column1 FROM t2);
```

在示例中，`SELECT * FROM t1 WHERE column1` 是外部查询（或外部语句），`(SELECT column1 FROM t2)` 是子查询。 子查询语句必须写在括号内，然后嵌套在外部查询语句中，也可以嵌套在其他子查询语句中，形成多层嵌套。

**子查询的主要优点**：

- 子查询可以划分语句，提供结构化查询。

- 子查询可替代复杂的 `JOIN` 和 `UNIONS` 语句。

- 子查询比复杂的 `JOIN` 和 `UNIONS` 可读性强。

**一个子查询有以下几类**：

- SELECT子查询
- FROM子查询
- WHERE子查询

更多信息，参见：

- [派生表](subqueries/derived-tables.md)
- [子查询与比较操作符的使用](subqueries/comparisons-using-subqueries.md)
- [子查询与 ANY 或 SOME 操作符的使用](subqueries/subquery-with-any-some.md)
- [子查询与 ALL 操作符的使用](subqueries/subquery-with-all.md)
- [子查询与 EXISTS 操作符的使用](subqueries/subquery-with-exists.md)
- [子查询与 IN 操作符的使用](subqueries/subquery-with-in.md)
