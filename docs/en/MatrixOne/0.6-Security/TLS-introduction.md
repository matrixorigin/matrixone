# TLS Introduction

Transport Layer Security (TLS) is a widely adopted security protocol designed to promote Internet communications' privacy and data security.

MatrixOne uses non-encrypted connections by default and supports enabling encrypted connections based on the TLS protocol. The supported protocol versions are TLS 1.0, TLS 1.1, and TLS 1.2.

To use encrypted connections, you need to enable encrypted connection support on the Matrix One server and specify the use of encrypted connections on the client side.

## Enable TLS for MatrixOne

1. Generate certificate and key

MatrixOne does not yet support loading password-protected private keys, so a private key file without a password must be provided. Certificates and keys can be signed and generated using OpenSSL. It is recommended to use the tool `mysql_ssl_rsa_setup` that comes with MySQL to generate quickly:

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

2. Modify TLS configuration of MatrixOne

To enable TLS support of MatrixOne, you need to modify the configuration information of `[cn.frontend]`. The configuration information is explained as follows:

|Parameter| Description|
|---|---|
|enableTls|Boolean type, whether to enable TLS support on the MO server. Defaults to false.|
|tlsCertFile|Specify the SSL certificate file path|
|tlsKeyFile|Specifies the private key corresponding to the certificate file|
|tlsCaFile|Optional. Specify trusted CA certificate file path|

An example of modifying the TLS configuration is as below:

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

If the configured file path or content is wrong, the MatrixOne server will fail to start.

3. Verify that MatrixOne's SSL is enabled

After logging in with the MySQL client, use the Status command to check whether SSL is enabled.

The value of the returned SSL will have a corresponding description. If it is not enabled, the returned result is `Not in use`:

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

If enabled, the result is:

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

## Configure MySQL client

When a MySQL client connects to Matrix One Server, the encrypted connection behavior needs to be specified by the `--ssl-mode` parameter, such as:

```sql
mysql -h 127.0.0.1 -P 6001 -udump -p111 --ssl-mode=PREFFERED
```

The value types of `ssl mode` are as follows:

|`ssl-mode` value|Description|
|---|---|
|DISABLED|Establish an encrypted connection without SSL/TLS, synonymous with skip-ssl.|
|PREFFERED|The default behavior is first to establish an encrypted connection using SSL/TLS; if it cannot be established, it will try to establish a non-SSL/TLS connection.|
|REQUIRED|Only SSL/TLS will be attempted to establish an encrypted connection, and if the connection cannot be established, the connection will fail.|
|VERIFY_CA|As with the REQUIRED behavior, and also verifies that the CA certificate on the Server side is valid.|
|VERIFY_IDENTITY|It acts like VERIFY_CA and verifies that the host in the server-side CA certificate is the same as the hostname for the actual connection.|

!!! note
    When the client specifies `--ssl-mode=VERIFY_CA`, it needs to use `--ssl-ca` to specify the CA certificate;
    If `--ssl-mode=VERIFY_IDENTITY` is specified on the client, you need to specify the CA certificate. You need to use `--ssl-key` to specify the client private key and `--ssl-cert` to specify the client certificate.
