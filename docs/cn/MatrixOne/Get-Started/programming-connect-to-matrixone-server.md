# 通过编程语言连接 MatrixOne 服务

MatrixOne 现在支持通过以下几种编程语言的方式连接 MatrixOne 服务：

- Java
- Python
- Golang

## 前期准备

已完成[安装并启动 MatrixOne](install-standalone-matrixone.md)。

## 通过 Java 语言的 JDBC 连接器连接 MatrixOne 服务

1. 下载安装 [Java 语言 JDBC 连接器](https://dev.mysql.com/downloads/connector/j/)。

2. 在应用程序中调用 `com.mysql.cj.jdbc.Driver` 驱动包即可。

## 通过 Python 语言的 pymysql 连接器连接 MatrixOne 服务

1. 下载安装 pymysql 连接器：

   ```
   pip3 install pymysql
   ```

2. 在应用程序中调用 `import pymysql` 即可，具体使用方式请参考 `pymysql` 官方文档：<https://pypi.org/project/PyMySQL/>。

!!! info
    关于通过 Python 语言的 pymysql 连接器连接 MatrixOne 服务的示例，参见[用 MatrixOne 构建一个简单的股票分析 Python 应用程序](../Develop/develop-python-application.md)。
