# Prepared 语句概述

MatrixOne 提供对服务器端预处理语句的支持。利用客户端或服务器二进制协议的高效性，对参数值使用带有占位符的语句进行预处理，执行过程中的优点如下：

- 每次执行语句时解析语句的效率提高。通常，数据库应用程序处理大量几乎相同的语句，只更改子句中的文字或变量值，例如用于查询和删除的 `WHERE`、用于更新的 `SET` 和用于插入的 `VALUES`。

- 防止 SQL 注入。参数值可以包含未转义的 SQL 引号和分隔符，一次编译，多次运行，省去了解析优化等过程。

## `PREPARE`、`EXECUTE`、和 `DEALLOCATE PREPAR`E 语句

PREPARE 语句的 SQL 基本语法主要为以下三种 SQL 语句：

- [PREPARE](prepare.md)：执行预编译语句。

- [EXECUTE](execute.md)：执行已预编译的句。

- [DEALLOCATE PREPARE](deallocate.md)：释放一条预编译的语句。
