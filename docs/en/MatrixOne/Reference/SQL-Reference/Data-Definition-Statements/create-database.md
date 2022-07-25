# **CREATE DATABASE**

## **Description**

Create a database.

## **Syntax**

```
> CREATE DATABASE [IF NOT EXISTS] <database_name> [create_option] ...

> create_option: [DEFAULT] {
	CHARACTER SET [=] charset_name
  | COLLATE [=] collation_name
  | ENCRYPTION [=] {'Y' | 'N'}
}
```

#### create_database_statement

![Create Database Diagram](https://github.com/matrixorigin/artwork/blob/main/docs/reference/create_database_statement.png?raw=true)

## **Examples**

```
> CREATE DATABASE test01;

> CREATE DATABASE IF NOT EXISTS test01;

> CREATE DATABASE test03 DEFAULT CHARACTER SET utf8 collate utf8_general_ci ENCRYPTION 'Y';

> CREATE DATABASE test04 CHARACTER SET=utf8 collate=utf8_general_ci ENCRYPTION='N';
```

## **Constraints**

- Only `UTF-8` CHARACTER SET is supported for now.
- `CHARACTER SET`, `COLLATE`, `ENCRYPTION` can be used but don't work.
