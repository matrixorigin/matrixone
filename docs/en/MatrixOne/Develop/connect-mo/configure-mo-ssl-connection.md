# How to configure MatrixOne SSL connection

## Overview

This document describes how to configure your MatrixOne server to use SSL for database connections. After you secure your MatrixOne connections, malicious users cannot intercept your traffic.

## Configure MatrixOne SSL connections

### Create the directory to store the SSL keys

To create the directory that will contain the SSL keys, perform the following steps:

1. Log into your server via SSH. Check if you have the **[mysql_ssl_rsa_setup](https://dev.mysql.com/doc/refman/5.7/en/mysql-ssl-rsa-setup.html)** tool in place.

    If you see this following message, it means you have installed it. If not, please [install MySQL](https://dev.mysql.com/doc/mysql-getting-started/en/) first, and this `mysql_ssl_rsa_setup` will be installed along.

    ```
    [pcusername@VM-0-12-centos matrixone]$ mysql_ssl_rsa_setup
    2022-10-19 10:57:30 [ERROR]   Failed to access directory pointed by --datadir. Please make sure that directory exists and is accessible by mysql_ssl_rsa_setup. Supplied value : /var/lib/mysql
    ```

2. Create an SSL key storage directory that MatrixOne can access. For example, run the `mkdir /home/user/mo_keys` command to create a `mo_keys` directory.

### Create the SSL keys

To create the SSL keys, perform the following steps:

1. Run the following commands to create the Certificate Authority (CA) keys:

    ```
    mysql_ssl_rsa_setup --datadir=/home/user/mo_keys
    ```

    You'll see in this folder a list of `.pem` files.

    /mo_keys
    ├── ca-key.pem
    ├── ca.pem
    ├── client-cert.pem
    ├── client-key.pem
    ├── private_key.pem
    ├── public_key.pem
    ├── server-cert.pem
    └── server-key.pem

2. Insert the following lines in the `[cn.frontend]` section of the `etc/launch-tae-logservice/cn1.toml` file in MatrixOne folder:

    ```
    [cn.frontend]
    enableTls = true
    tlsCertFile = "/home/user/mo_keys/server-cert.pem"
    tlsKeyFile = "/home/user/mo_keys/server-key.pem"
    tlsCaFile = "/home/user/mo_keys/ca.pem"
    ```

    If `[cn.frontend]` section doesn't exist in the MatrixOne system setting file, you can just create one with the above settings.

### Test the SSL configuration

To test the SSL configuration, perform the following steps:

1. Launch MatrixOne service. Please refer to [Installed and launched MatrixOne](../../Get-Started/install-standalone-matrixone.md).
2. Connect to MatrixOne service by MySQL client, please refer to [Connect to MatrixOne](../../Get-Started/connect-to-matrixone-server.md).

    ```
    mysql -h IP_ADDRESS -P 6001 -udump -p111
    ```

3. After you connect, run the `status` command. The output will resemble the following example:

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
