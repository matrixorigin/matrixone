# **连接MatrixOne服务**

## **准备工作**

请确保你已经完成了单机版MatrixOne或者分布式MatrixOne集群的安装部署。

[安装单机版MatrixOne](install-standalone-matrixone.md)

[部署分布式MatrixOne集群](install-distributed-matrixone.md)

## **1. 安装MySQL客户端**
   
MatrixOne支持MySQL连接协议，因此您可以使用各种语言通过MySQL客户机程序进行连接。

目前，MatrixOne只兼容Oracle MySQL客户端，因此一些特性可能无法在MariaDB、Percona客户端下正常工作。

## **2. 连接MatrixOne服务**

你可以使用MySQL命令行客户端来连接MatrixOne服务。

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

目前，MatrixOne只支持TCP监听。
