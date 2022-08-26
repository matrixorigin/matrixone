# 编程语言连接 MatrixOne 服务

MatrixOne 现在支持通过以下几种编程语言的方式连接 MatrixOne 服务：

- Java
- Python

## 前期准备

已完成[安装并启动 MatrixOne](../../Get-Started/install-standalone-matrixone.md)。

## Java 语言的 JDBC 连接器连接 MatrixOne 服务

1. 下载安装 [Java 语言 JDBC 连接器](https://dev.mysql.com/downloads/connector/j/)。

2. 在应用程序中调用 `com.mysql.cj.jdbc.Driver` 驱动包即可。

    !!! info
        关于通过 Java 语言的 JDBC 连接器连接 MatrixOne  服务的示例，参见[用 JDBC 连接器连接 MatrixOne 服务](../../Tutorial/develop-java-connect-mo.md)。

## Python 语言的 pymysql 工具连接 MatrixOne 服务

1. 下载安装 pymysql 工具：

    ```
    pip3 install pymysql
    ```

2. 在应用程序中调用 `import pymysql` 即可，具体使用方式请参考 `pymysql` 官方文档：<https://pypi.org/project/PyMySQL/>。

    !!! info
         关于通过 Python 语言的 pymysql 工具连接 MatrixOne  服务的示例，参见[用 MatrixOne 构建一个简单的股票分析 Python 应用程序](../../Tutorial/develop-python-application.md)。

<!--执行py脚本，1. python 脚本。或者是编程环境下，或者是终端，执行python脚本；2. 相应的数据库建表 3. python脚本完成对数据库的执行（操作） python3+python文件名称完成对数据库的操作-->
