# Create a Table

This document introduces how to create tables using the SQL statement and the related best practices. In the previous document, you created a database named *modatabase*. In this document, you will create a table in the database.

## Before you start

Before reading this document, make sure that the following tasks are completed:

- Build a MatrixOne Cluster in MatrixOne.
- Read the [Database Schema Design Overview](overview.md).
- The database has been created.

## What is a table

A table is a logical object in MatrixOne cluster that is subordinate to the database. It is used to store the data.

Tables save data records in the form of rows and columns. A table has at least one column. If you have defined n columns, each row of data has exactly the same fields as the n columns.

## Name a table

The first step for creating a table is to give your table a name. Do not use meaningless names that will cause great distress to yourself or your colleagues in the future. It is recommended that you follow your company or organization's table naming convention.

The `CREATE TABLE` statement usually takes the following form:

```sql
CREATE TABLE {table_name} ({elements});
```

For more information on `CREATE TABLE` statement, see [CREATE TABLE](../../Reference/SQL-Reference/Data-Definition-Statements/create-table.md).

**Parameter description**

- {table_name}: The name of the table to be created.

- {elements}: A comma-separated list of table elements, such as column definitions and primary key definitions.

## Define columns

A column is subordinate to a table. Each table has at least one column. Columns provide a structure to a table by dividing the values in each row into small cells of a single data type.

Column definitions typically take the following form.

```
{column_name} {data_type} {column_qualification}
```

**Parameter description**

- {column_name}: The column name.
- {data_type}: The column data type.
- {column_qualification}: Column qualifications.

Suppose you need to create a table to store the *user* information in the *modatabase* database.

```sql
CREATE TABLE `modatabase`.`users` (
  `id` bigint,
  `nickname` varchar(100),
  `balance` decimal(15,2)
);
```

**Explanations**

The following table explains the fields in the above example:

|Filed Name|Data Types|Function|Description|
|---|---|---|---|
|id|bigint|This is used to represent a unique user identifier.| This means that all user identifiers should be of the bigint type.|
|nickname|varchar|user's nickname|The length limit of 100 characters|
|balance|decimal|Balance of user|decimal(5,2) means a precision of 5 and a scale of 2, with the range from -999.99 to 999.99. decimal(6,1) means a precision of 6 and a scale of 1, with the range from -99999.9 to 99999.9. decimal is a fixed-point types, which can be used to store numbers accurately.|

MatrixOne supports many other column data types, including the integer types, floating-point types, date and time types. For more information, see [Data Types](../../Reference/Data-Types/data-types.md).

**Create a complex table**

Create a *books* table which will be the core of the modatabase data. The *books* table contains fields for the book's ids, titles, stock, prices, and publication dates.

```sql
CREATE TABLE `modatabase`.`books` (
  `id` bigint NOT NULL,
  `title` varchar(100),
  `published_at` datetime,
  `stock` int,
  `price` decimal(15,2)
);
```

This table contains more data types than the users table.

|Filed Name|Data Types|Function|Description|
|---|---|---|---|
|stock|int|The type of right size is recommended |To avoid using too much disk or even affecting performance (too large a type range) or data overflow (too small a data type range).|
|published_at|datetime|publication dates|The datetime type can be used to store time values.|

## Select primary key

A primary key is a column or a set of columns in a table whose values uniquely identify a row in the table.

The primary key is defined in the `CREATE TABLE` statement. The primary key constraint requires that all constrained columns contain only `non-NULL` values.

A table can be created without a primary key or with a non-integer primary key.

### Set default value

To set a default value on a column, use the `DEFAULT` constraint. The default value allows you to insert data without specifying a value for each column.

You can use `DEFAULT` together with supported SQL functions to move the calculation of defaults out of the application layer, thus saving resources of the application layer. The resources consumed by the calculation do not disappear and are moved to the MatrixOne cluster. Commonly, you can insert data with the default time. The following exemplifies setting the default value in the *ratings* table:

```sql
CREATE TABLE `modatabase`.`ratings` (
  `book_id` bigint,
  `user_id` bigint,
  `score` tinyint,
  `rated_at` datetime DEFAULT NOW(),
  PRIMARY KEY (`book_id`,`user_id`)
);
```

### Prevent duplicate values

If you need to prevent duplicate values in a column, you can use the `UNIQUE` constraint.

For example, to make sure that users' nicknames are unique, you can rewrite the table creation SQL statement for the *users* table like this:

```sql
CREATE TABLE `modatabase`.`users` (
  `id` bigint,
  `balance` decimal(15,2),
  `nickname` varchar(100) UNIQUE,
  PRIMARY KEY (`id`)
);
```

If you try to insert the same nickname in the *users* table, an error is returned.

### Prevent null values

If you need to prevent null values in a column, you can use the `NOT NULL` constraint.

Take user nicknames as an example. To ensure that a nickname is not only unique but is also not null, you can rewrite the SQL statement for creating the *users* table as follows:

```sql
CREATE TABLE `modatabase`.`users` (
  `id` bigint,
  `balance` decimal(15,2),
  `nickname` varchar(100) UNIQUE NOT NULL,
  PRIMARY KEY (`id`)
);
```

## Execute the CREATE TABLE statement

To view all tables under the *modatabase* database, use the `SHOW TABLES` statement.

```sql
SHOW TABLES IN `modatabase`;
```

Running results:

```
+----------------------+
| tables_in_modatabase |
+----------------------+
| books                |
| ratings              |
| users                |
+----------------------+
```

## Guidelines to follow when creating a table

This section provides guidelines you need to follow when creating a table.

### Guidelines to follow when naming a table

- Use a fully-qualified table name (for example, `CREATE TABLE {database_name}. {table_name}`). If you do not specify the database name, MatrixOne uses the current database in your SQL session. If you do not use `USE {databasename};` to specify the database in your SQL session, MatrixOne returns an error.

- Use meaningful table names. For example, if you need to create a *user* table, you can use names: *user*, *t_user*,*users*, or follow your company or organization's naming convention.

- Multiple words are separated by an underscore, and it is recommended that the name is no more than 32 characters.

- Create a separate *DATABASE* for tables of different business modules and add comments accordingly.

### Guidelines to follow when defining columns

- Check the data types supported.

- Check the guidelines to follow for selecting primary keys and decide whether to use primary key columns.

- Check adding column constraints and decide whether to add constraints to the columns.

- Use meaningful column names. It is recommended that you follow your company or organization's table naming convention. If your company or organization does not have a corresponding naming convention, refer to the column naming convention.

### Guidelines to follow when selecting primary key

- Define a primary key or unique index within the table.
- Try to select meaningful columns as primary keys.
- For performance reasons, try to avoid storing extra-wide tables. It is not recommended that the number of table fields is over 60 and that the total data size of a single row is over 64K. It is recommended to split fields with too much data length to another table.
- It is not recommended to use complex data types.
- For the fields to be joined, ensure that the data types are consistent and avoid implicit conversion.
- Avoid defining primary keys on a single monotonic data column. If you use a single monotonic data column (for example, a column with the AUTO_INCREMENT attribute) to define the primary key, it might impact the write performance.
