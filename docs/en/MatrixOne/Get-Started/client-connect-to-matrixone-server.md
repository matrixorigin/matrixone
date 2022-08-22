# Connect to the MatrixOne server through a client

MatrixOne now supports the following client connections to the MatrixOne server:

- MySQL Shell
- Navicat

## Before you start

Make sure you have already [installed and launched MatrixOne](install-standalone-matrixone.md).

## Connect to the MatrixOne Server through MySQL Client

1. Download and install [MySQL Client](https://dev.mysql.com/downloads/installer/)。

2. Connect to the MatrixOne server.

   ```
   mysql -h 127.0.0.1 -P 6001 -udump -p
   ```

   The successful result is as below:

   ```
   Welcome to the MySQL monitor. Commands end with ; or \g. Your MySQL connection id is 1031
   Server version: 0.5.0 MatrixOne
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

3. Click **Save**, save the configuration。

4. To connect to the MatrixOne server, double-click **MatrixOne** in the database directory on the left.
