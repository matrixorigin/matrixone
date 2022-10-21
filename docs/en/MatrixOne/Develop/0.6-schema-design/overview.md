# Database Schema Design Overview

This document provides the basics of MatrixOne database schema design. This document introduces terminology related to MatrixOne databases and subsequent data read and write examples.

## Key concept in MatrixOne

Database Schema: The database schema mentioned in this article is the same as the logical object database. It is the same as MySQL.

## Database

A database in MatrixOne is a collection of objects such as tables.

To view the default database contained by MatrixOne, ues `SHOW DATABASES;` statment.

To create a new database, ues `CREATE DATABASE database_name;` statement.

## Table

A table is a collection of related data in a database.

Each table consists of rows and columns. Each value in a row belongs to a specific column. Each column allows only a single data type. To further qualify columns, you can add some constraints.

## Other supported logical objects

MatrixOne supports the following logical objects at the same level as table:

- View: a view acts as a virtual table, whose schema is defined by the SELECT statement that creates the view.

- Temporary table: a table whose data is not persistent.

## Access Control

MatrixOne supports both user-based and role-based access control. To allow users to view, modify, or delete data, for more information, see [Access control in MatrixOne](../../Security/access-control-overview.md).

## Object limitations

### Limitations on identifier length

|Identifier type|Maximum length (number of characters allowed)|
|---|---|
|Database|64|
|Table|64|
|Column|64|
|Sequence|64|

### Limitations on a single table

|Type|Upper limit (default value)|
|---|---|
|Columns|Defaults to 1017 and can be adjusted up to 4096|
|Partitions|8192|
|Size of a single line|6 MB by default|
|Size of a single column|6 MB|

### Limitations on data types

For more information on data types, see [Data Types](../../Reference/Data-Types/data-types.md).

### Number of rows

MatrixOne supports an unlimited number of rows by adding nodes to the cluster.
