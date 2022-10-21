# **CREATE TEMPORARY TABLE**

## **语法说明**

在创建表时，可以使用 `TEMPORARY` 关键字。`TEMPORARY` 表只在当前会话中可见，在会话关闭时自动删除。这表示两个不同的会话可以使用相同的临时表名，而不会彼此冲突或与同名的现有非临时表冲突。(在删除临时表之前，会隐藏现有表。)

删除数据库会自动删除数据库中创建的所有 `TEMPORARY` 表。

创建会话可以对表执行任何操作，例如 `DROP table`、`INSERT`、`UPDATE` 或 `SELECT`。
