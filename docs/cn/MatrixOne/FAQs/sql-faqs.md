# **SQL 常见问题**

* **MatrixOne中的函数和关键字是否区分大小写？**

  不区分大小写。

  在 MatrixOne 中，只有一种情况需要区分大小写：如果你创建的表和属性带有 \`\`，\`\` 中的名称需要注意大小写。查询这个表名或属性名，那么表名和属性名也需要被包含在\`\`里。

* **如何将数据从 MatrixOne 导出到文件?**

你可以使用 `SELECT INTO OUTFILE` 命令来将数据导出为 **csv** 文件（只能导出到服务器主机，无法到远程客户端）。  
关于该命令的更多信息，参见[SELECT参考指南](../Reference/SQL-Reference/Data-Manipulation-Statements/select.md)。

* **MatrixOne 事务大小的限制是什么？**

  事务大小受限于硬件环境的内存大小。

* **MatrixOne支持什么类型的字符集？**

  MatrixOne 默认支持 UTF-8 字符集，且目前只支持 UTF-8。

* **MatrixOne中的 `sql_mode` 是什么？**

  MatrixOne 默认的 `sql_mode` 是 MySQL 中的 `only_full_group_by` 。目前 MatrixOne 不支持修改 `sql_mode`。

* **我如何批量将数据加载到 MatrixOne？**

  MatrixOne提供了两种批量加载数据的方法：
  - 在 shell 中使用 `source filename` 命令，你可以加载包含所有 DDL 的 SQL 文件并插入数据语句。
  - 使用 `load data infile...into table...` 命令，你可以加载一个现有的 *.csv* 文件到 MatrixOne。

* **我怎么知道我的查询是如何执行的？**

  要查看 MatrixOne 对给定查询的执行情况，可以使用[`EXPLAIN`](https://docs.matrixorigin.io/0.5.0/MatrixOne/Reference/SQL-Reference/Explain/explain/)语句，它将打印出查询计划。

  ```
  EXPLAIN SELECT col1 FROM tbl1;
  ```
