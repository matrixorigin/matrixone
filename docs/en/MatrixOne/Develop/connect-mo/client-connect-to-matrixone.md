# Connecting to MatrixOne with Database Client Tool

MatrixOne now supports the following Database client tools:

- MySQL Shell
- Navicat
- DBeaver

## Before you start

Make sure you have already [installed and launched MatrixOne](../../Get-Started/install-standalone-matrixone.md).

## Connect to the MatrixOne Server through MySQL Client

1. Download and install [MySQL Client](https://dev.mysql.com/downloads/installer/)。

2. Connect to the MatrixOne server.

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

    The successful result is as below:

    ```
    Welcome to the MySQL monitor. Commands end with ; or \g. Your MySQL connection id is 1031
    Server version: 0.5.1 MatrixOne
    Copyright (c) 2000, 2022, Oracle and/or its affiliates.

    Oracle is a registered trademark of Oracle Corporation and/or its affiliates. Other names may be trademarks of their respective owners.
    Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.
    ```

For more information on deployment, see [Deployment FAQs](../../FAQs/deployment-faqs.md).

## Connect to the MatrixOne Server through Navicat

1. Download and install [Navicat](https://www.navicat.com/en/products).

2. Open Navicat, click **Connection > MySQL**, and fill in the following parameters in the pop-up window:

    - **Connction Name**: MatrixOne
    - **Host**: 127.0.0.1
    - **Port**: 6001
    - **User Name**: dump
    - **Password**: 111
    - **Save password**：Yes

3. Click **Save**, save the configuration.

4. To connect to the MatrixOne server, double-click **MatrixOne** in the database directory on the left.

## Connect to the MatrixOne Server through DBeaver

1. Download and install [DBeaver](https://dbeaver.io/download/).

2. Open DBeaver, click **Connection**, select **MySQL**, then click **Next**, and fill in the following parameters in the pop-up window:

    - **Host**: 127.0.0.1
    - **Port**: 6001
    - **Connction Name**: MatrixOne
    - **User Name**: dump
    - **Password**: 111
    - **Save password**：Yes

3. Click **Save**, save the configuration.

4. Right-click **MatrixOne** in database navigation on the left select **Edit link**, modify the configuration in the **Driver Attribute** area of the **Connection Settings**:

    ```
    - characterSetResults: "utf8"
    - continueBatchOnError: "false"
    - useServerPrepStmts: "true"
    - alwaysSendSetIsolation: "false"
    - useLocalSessionState: "true"
    - zeroDateTimeBehavior: "CONVERT_TO_NULL"
    - failoverReadOnly: "false"
    - serverTimezone: "Asia/Shanghai"
    - socketTimeout: 30000
    ```

5. To connect to the MatrixOne server, double-click **MatrixOne** in the database navigation on the left.
