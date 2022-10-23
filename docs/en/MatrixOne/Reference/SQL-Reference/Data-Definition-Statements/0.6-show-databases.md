# **SHOW DATABASES**

## **Description**

`SHOW DATABASES` lists the databases on the MatrixOne. `SHOW SCHEMAS` is a synonym for `SHOW DATABASES`. The LIKE clause, if present, indicates which database names to match. The WHERE clause can be given to select rows using more general conditions.

MatrixOne implements databases as directories in the data directory, so this statement simply lists directories in that location.

Database information is also available from the `INFORMATION_SCHEMA` SCHEMATA table.

## **Syntax**

```
> SHOW {DATABASES | SCHEMAS}
    [LIKE 'pattern' | WHERE expr]
```

## **Examples**

```sql
> create database demo_1;
> show databases;
+--------------------+
| Database           |
+--------------------+
| mo_catalog         |
| system             |
| system_metrics     |
| information_schema |
| demo_1             |
+--------------------+
7 rows in set (0.00 sec)
