# Python 连接 MatrixOne 服务

MatrixOne 支持 Python 连接 MatrixOne 服务。

!!! Note
    MatrixOne 0.5.0 和 0.5.1 版本仅支持 `pymysql` 工具连接，暂不支持 `sqlalchemy` 和 `mysql-connector`。

## 前期准备

已完成[安装并启动 MatrixOne](../../Get-Started/install-standalone-matrixone.md)。

## Python 语言的 pymysql 工具连接 MatrixOne 服务

1. 下载安装 pymysql 工具：

    ```
    pip3 install pymysql
    ```

2. 在应用程序中调用 `import pymysql` 即可，具体使用方式请参考 `pymysql` 官方文档：<https://pypi.org/project/PyMySQL/>。

## 参考文档

更多关于 Python 连接 MatrixOne 服务实践教程，参见[用 MatrixOne 构建一个简单的股票分析 Python 应用程序](../../Tutorial/develop-python-application.md)。
