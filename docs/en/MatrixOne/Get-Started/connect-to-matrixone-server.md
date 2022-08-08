# **Connect to MatrixOne Server**

## **Before you begin**

Make sure you have already [installed MatrixOne](install-standalone-matrixone.md).

## **1. Install MySQL client**

MatrixOne supports the MySQL wire protocol, so you can use MySQL client drivers to connect from various languages.

Currently, MatrixOne is only compatible with the Oracle MySQL client. This means that some features might not work with the MariaDB client or Percona client.

## **2. Connect to MatrixOne server**

You can use the MySQL command-line client to connect to MatrixOne server:

```
mysql -h IP -P PORT -uUsername -p
```

The connection string is the same format as MySQL accepts. You need to provide a user name and a password.

Use the built-in test account for example:

- user: dump
- password: 111

```
mysql -h 127.0.0.1 -P 6001 -udump -p
Enter password:
```

Currently, MatrixOne only supports the TCP listener.
