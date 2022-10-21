# Create a Database

This document describes how to create a database using SQL and various programming languages and lists the rules of database creation. In this document, the modatabase application is taken as an example to walk you through the steps of database creation.

## Before you start

Before creating a database, do the following:

- Build a MatrixOne Cluster in MatrixOne.
- Read the [Database Schema Design Overview](overview.md).

## What is database

Database objects in MatrixOne contain tables, views and other objects.

## Create databases

To create a database, you can use the `CREATE DATABASE` statement.

```sql
CREATE DATABASE IF NOT EXISTS `modatabase`;
```

For more information on `CREATE DATABASE` statement, see [CREATE DATABASE](../../Reference/SQL-Reference/Data-Definition-Statements/create-database.md).

## View databases

To view the databases in a cluster, use the `SHOW DATABASES` statement.

```sql
SHOW DATABASES;
```

The following is an example output:

```
+--------------------+
| Database           |
+--------------------+
| mo_catalog         |
| system             |
| system_metrics     |
| mysql              |
| information_schema |
| modatabase         |
+--------------------+
```

## Rules in database creation

- Follow the Database Naming Conventions and name your database meaningfully.

- You can create your own database using the `CREATE DATABASE` statement and change the current database using the `USE {databasename};` statement in a SQL session.

- Use the root user to create objects such as database, roles, and users. Grant only the necessary privileges to roles and users. For more information, see [Access control in MatrixOne](../../Security/access-control-overview.md).
