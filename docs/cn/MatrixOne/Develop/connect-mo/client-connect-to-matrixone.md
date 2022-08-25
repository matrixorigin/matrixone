# 客户端连接 MatrixOne 服务

MatrixOne 现在支持通过以下几种客户端的方式连接 MatrixOne 服务：

- MySQL Client
- Navicat

## 前期准备

已完成[安装并启动 MatrixOne](../../Get-Started/install-standalone-matrixone.md)。

## 通过 MySQL Client 连接 MatrixOne 服务

1. 下载安装 [MySQL Client](https://dev.mysql.com/downloads/installer/)。

2. 下载完成后，你可以使用 MySQL 命令行客户端来连接 MatrixOne 服务。

    ```
    mysql -h IP -P PORT -uUsername -p
    ```

    连接符的格式与MySQL格式相同，你需要提供用户名和密码。

    此处以内置帐号作为示例：

    - user: dump
    - password: 111

    ```
    mysql -h 127.0.0.1 -P 6001 -udump -p
    Enter password:
    ```

3. 连接成功提示如下：

    ```
    Welcome to the MySQL monitor. Commands end with ; or \g. Your MySQL connection id is 1031
    Server version: 0.5.0 MatrixOne
    Copyright (c) 2000, 2022, Oracle and/or its affiliates.

    Oracle is a registered trademark of Oracle Corporation and/or its affiliates. Other names may be trademarks of their respective owners.
    Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.
    ```

更多关于安装部署的问题，参见[部署常见问题](../../FAQs/deployment-faqs.md)。

## 通过 Navicat 连接 MatrixOne 服务

1. 下载安装 [Navicat](https://www.navicat.com/en/products)。

2. 安装 Navicat 完成后，打开 Navicat，点击左上角 **Connection > MySQL**， 在弹窗中填入如下参数：

    - **Connction Name**: MatrixOne
    - **Host**: 127.0.0.1
    - **Port**: 6001
    - **User Name**: dump
    - **Password**: 111
    - **Save password**：勾选

3. 点击 **Save** 保存设置。

4. 双击左侧数据库目录中的 **MatrixOne**，图标点亮，连接成功。
