# 如何配置 MatrixOne SSL 连接

## 概述

本文介绍如何配置 SSL 安全连接 MatrixOne 服务器。在传送信息时，采用 SSL 连接的方式，可以避免恶意用户拦截你的流量。

## 配置 MatrixOne SSL 连接

### 新建目录存储 SSL 密钥

创建包含 SSL 密钥的目录，执行以下步骤：

1. 通过 SSH 登录 MatrixOne 服务，先确认你已安装 **[mysql_ssl_rsa_setup](https://dev.mysql.com/doc/refman/5.7/en/mysql-ssl-rsa-setup.html)** 工具。

    检查你是否安装 MySQL：如果你已安装 MySQL，则出现以下代码，如果没有如下代码，安装 MySQL 参见[install MySQL](https://dev.mysql.com/doc/mysql-getting-started/en/)，`mysql_ssl_rsa_setup` 也将一并安装。

    ```
    [pcusername@VM-0-12-centos matrixone]$ mysql_ssl_rsa_setup
    2022-10-19 10:57:30 [ERROR]   Failed to access directory pointed by --datadir. Please make sure that directory exists and is accessible by mysql_ssl_rsa_setup. Supplied value : /var/lib/mysql
    ```

2. 创建一个 MatrixOne 可以访问的 SSL 密钥存储目录。例如，执行命令 `mkdir /home/user/mo_keys` 创建目录 *mo_keys*。

### 创建 SSL 密钥

执行以下步骤创建 SSL 密钥：

1. 运行命令创建 CA (Certificate Authority) 密钥：

    ```
    mysql_ssl_rsa_setup --datadir=/home/user/mo_keys
    ```

    文件夹将创建产生一个 *.pem* 文件。

    /mo_keys
    ├── ca-key.pem
    ├── ca.pem
    ├── client-cert.pem
    ├── client-key.pem
    ├── private_key.pem
    ├── public_key.pem
    ├── server-cert.pem
    └── server-key.pem

2. 在 MatrixOne 目录下的 *etc/launch-tae-logservice/cn1.toml* 文件内的 `[cn.front]` 部分插入以下代码段：

    ```
    [cn.frontend]
    enableTls = true
    tlsCertFile = "/home/user/mo_keys/server-cert.pem"
    tlsKeyFile = "/home/user/mo_keys/server-key.pem"
    tlsCaFile = "/home/user/mo_keys/ca.pem"
    ```

    如果 `[cn.frontend]` 部分在 MatrixOne 系统设置文件中不存在，你可以用上述设置创建一个。

### 测试 SSL 配置是否成功

执行以下步骤，测试测试 SSL 配置是否成功：

1. 安装构建 MatrixOne 服务，具体步骤，参见[安装单机版 MatrixOne](../../Get-Started/install-standalone-matrixone.md)。
2. 通过 MySQL 客户端连接 MatrixOne 服务，具体步骤，参见 [连接 MatrixOne](../../Get-Started/connect-to-matrixone-server.md)。

    ```
    mysql -h IP_ADDRESS -P 6001 -udump -p111
    ```

3. 连接完成后，运行 `status` 命令，输出结果示例如下：

    ```
    mysql> status
    --------------
    mysql  Ver 8.0.28 for Linux on x86_64 (MySQL Community Server - GPL)

    Connection id:		1001
    Current database:
    Current user:		dump@0.0.0.0
    SSL:			Cipher in use is TLS_AES_128_GCM_SHA256
    Current pager:		stdout
    Using outfile:		''
    Using delimiter:	;
    Server version:		0.5.1 MatrixOne
    Protocol version:	10
    Connection:		127.0.0.1 via TCP/IP
    Client characterset:	utf8mb4
    Server characterset:	utf8mb4
    TCP port:		6002
    Binary data as:		Hexadecimal
    --------------
    ```
