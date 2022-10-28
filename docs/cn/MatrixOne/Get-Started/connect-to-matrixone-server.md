# **连接 MatrixOne 服务**

## **准备工作**

请确保你已经完成了单机版 MatrixOne 的安装部署。

[安装单机版MatrixOne](install-standalone-matrixone.md)

## **1. 安装 MySQL 客户端**

MatrixOne 支持 MySQL 连接协议，因此您可以使用各种语言通过MySQL客户机程序进行连接。关于 MySQL 客户端连接 MatrixOne，参见[通过客户端连接 MatrixOne 服务](../Develop/connect-mo/client-connect-to-matrixone.md)。

目前，MatrixOne只兼容 Oracle MySQL 客户端，因此一些特性可能无法在 MariaDB、Percona 客户端下正常工作。

## **2. 连接 MatrixOne 服务**

你可以使用 MySQL 命令行客户端来连接 MatrixOne 服务。

```
mysql -h IP -P PORT -uUsername -p
```

连接符的格式与MySQL格式相同，您需要提供用户名和密码。

此处以内置帐号作为示例：

- user: dump
- password: 111

```
mysql -h 127.0.0.1 -P 6001 -udump -p
Enter password:
```

目前，MatrixOne 只支持TCP监听。

更多有关连接 MatrixOne 的方式，参见[客户端连接 MatrixOne 服务](../Develop/connect-mo/client-connect-to-matrixone.md)、[JDBC 连接 MatrixOne 服务](../Develop/connect-mo/java-connect-to-matrixone/connect-mo-with-jdbc.md)和[Python 连接 MatrixOne 服务](../Develop/connect-mo/python-connect-to-matrixone.md)。
