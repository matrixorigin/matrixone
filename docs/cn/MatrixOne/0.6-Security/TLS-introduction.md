# 开启 TLS

传输层安全性 (Transport Layer Security, TLS) 是一种广泛采用的安全性协议，旨在促进互联网通信的私密性和数据安全性。

MatrixOne 默认采用非加密连接，也支持启用基于 TLS 协议的加密连接，支持的协议版本有 TLS 1.0, TLS 1.1, TLS 1.2。

使用加密连接需要在 MatrixOne 服务端开启加密连接支持，并在客户端指定使用加密连接。

## 开启 MatrixOne 的 TLS 支持

1. 生成证书及密钥

MatrixOne 尚不支持加载有密码保护的私钥，因此必须提供一个没有密码的私钥文件。证书和密钥可以使用 OpenSSL 签发和生成，推荐使用 MySQL 自带的工具 `mysql_ssl_rsa_setup` 快捷生成：

```
mysql_ssl_rsa_setup --datadir=./yourpath
yourpath
├── ca-key.pem
├── ca.pem
├── client-cert.pem
├── client-key.pem
├── private_key.pem
├── public_key.pem
├── server-cert.pem
└── server-key.pem
```

2. 修改 MatrixOne 的 TLS 配置

打开 MatrixOne 的 TLS 支持，需要修改 `[cn.frontend]` 的配置信息，配置信息解释如下：

|参数|描述|
|---|---|
|enableTls|布尔类型，是否在MO服务端打开TLS的支持。默认为false。|
|tlsCertFile|指定 SSL 证书文件路径|
|tlsKeyFile|指定证书文件对应的私钥|
|tlsCaFile|可选，指定受信任的 CA 证书文件路径|

修改 TLS 配置示例如下：

```
[cn.frontend]
#default is false. With true. Server will support tls
enableTls = true

#default is ''. Path of file that contains X509 certificate in PEM format for client
tlsCertFile = "yourpath/server-cert.pem"

#default is ''. Path of file that contains X509 key in PEM format for client
tlsKeyFile = "yourpath/server-key.pem"

#default is ''. Path of file that contains list of trusted SSL CAs for client
tlsCaFile = "yourpath/ca.pem"
```

如果配置的文件路径或内容错误，则 MatrixOne 服务将无法启动。

3. 验证 MatrixOne 的 SSL 是否启用

使用 MySQL 客户端登录后通过 Status 命令查看 SSL 是否启用。

返回的SSL的值会有相应说明，如果没有启用，返回结果为 `Not in use`：

```sql
mysql -h 127.0.0.1 -P 6001 -udump -p111

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql> status
--------------
/usr/local/mysql/bin/mysql  Ver 8.0.30 for macos12 on arm64 (MySQL Community Server - GPL)

Connection id:		1001
Current database:
Current user:		dump@0.0.0.0
SSL:			    Not in use
Current pager:		stdout
Using outfile:		''
Using delimiter:	;
Server version:		0.5.1 MatrixOne
Protocol version:	10
Connection:		127.0.0.1 via TCP/IP
ERROR 1105 (HY000): the system variable does not exist
ERROR 2014 (HY000): Commands out of sync; you can't run this command now
Client characterset:	utf8mb4
Server characterset:	utf8mb4
TCP port:		6001
Binary data as:		Hexadecimal
--------------
```

如果启用，结果为：

```sql
mysql -h 127.0.0.1 -P 6001 -udump -p111

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql> status
mysql  Ver 8.0.28 for macos11 on arm64 (MySQL Community Server - GPL)

Connection id:          1001
Current database:
Current user:           dump@0.0.0.0
SSL:                    Cipher in use is TLS_AES_128_GCM_SHA256
Current pager:          stdout
Using outfile:          ''
Using delimiter:        ;
Server version:         0.5.0 MatrixOne
Protocol version:       10
Connection:             127.0.0.1 via TCP/IP
ERROR 20101 (HY000): internal error: the system variable does not exist
ERROR 2014 (HY000): Commands out of sync; you can't run this command now
Client characterset:    utf8mb4
Server characterset:    utf8mb4
TCP port:               6001
Binary data as:         Hexadecimal
--------------
```

## MySQL 客户端配置

MySQL 客户端连接 Matrix One Server 时，需要通过 `--ssl-mode` 参数指定加密连接行为，如：

```sql
mysql -h 127.0.0.1 -P 6001 -udump -p111 --ssl-mode=PREFFERED
```

ssl mode 取值类型如下：

|ssl-mode 取值|含义|
|---|---|
|DISABLED|不使用 SSL/TLS 建立加密连接，与 skip-ssl 同义。|
|PREFFERED|默认行为，优先尝试使用 SSL/TLS 建立加密连接，如果无法建则尝试建立非 SSL/TLS 连接。|
|REQUIRED|只会尝试使用 SSL/TLS 建立加密连接，如果无法建立连接，则会连接失败。|
|VERIFY_CA|与 REQUIRED 行为一样，并且还会验证 Server 端的 CA 证书是否有效。|
|VERIFY_IDENTITY|与 VERIFY_CA 行为一样，并且还验证 Server 端 CA 证书中的 host 是否与实际连接的 hostname 是否一致。|

!!! note
    客户端在指定了 `--ssl-mode=VERIFY_CA` 时，需要使用 `--ssl-ca` 来指定 CA 证书；
    客户端在指定了 `--ssl-mode=VERIFY_IDENTITY` 时，需要指定 CA 证书，且需要使用 `--ssl-key` 指定客户端的私钥和使用 `--ssl-cert` 指定客户端的证书。
