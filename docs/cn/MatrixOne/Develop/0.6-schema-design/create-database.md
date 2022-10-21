# 创建数据库

本篇文档中介绍如何使用 SQL 来创建数据库，及创建数据库时应遵守的规则。

!!! note
    此处仅对 `CREATE DATABASE` 语句进行简单描述。更多信息，参见 [CREATE DATABASE](../../Reference/SQL-Reference/Data-Definition-Statements/create-database.md)。

## 开始前准备

在阅读本页面之前，你需要准备以下事项：

- 了解并已经完成构建 MatrixOne 集群。
- 了解什么是[数据库模式](overview.md)。

## 什么是数据库

在 MatrixOne 中数据库对象可以包含表、视图等对象。

## 创建数据库

可使用 `CREATE DATABASE` 语句来创建数据库。

```sql
CREATE DATABASE IF NOT EXISTS `modatabase`;
```

此语句会创建一个名为 *modatabase* 的数据库（如果尚不存在）。

要查看集群中的数据库，可在命令行执行一条 `SHOW DATABASES` 语句：

```sql
SHOW DATABASES;
```

运行结果为：

```
+--------------------+
| Database           |
+--------------------+
| mo_catalog         |
| system             |
| system_metrics     |
| mysql              |
| information_schema |
| modatabase         |
+--------------------+
```

## 数据库创建时应遵守的规则

- 给你的数据库起一个有意义的名字。

- 你可以使用 `CREATE DATABASE` 语句来创建数据库，并且在 SQL 会话中使用 `USE {databasename};` 语句来使用你所创建的数据库。

- 使用 root 用户创建数据库、角色、用户等，并只赋予必要的权限，参见[MatrixOne 中的权限控制](../../Security/access-control-overview.md)。

- 你已经准备完毕 *modatabase* 数据库，可以将表添加到该数据库中，参见下一章节[创建表](create-table.md)。
