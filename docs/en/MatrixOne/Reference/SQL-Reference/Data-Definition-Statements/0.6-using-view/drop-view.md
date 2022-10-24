# **DROP VIEW**

## **Description**

`DROP VIEW` removes one or more views.

If any views named in the argument list do not exist, the statement fails with an error indicating by name which nonexisting views it was unable to drop, and no changes are made.

The `IF EXISTS` clause prevents an error from occurring for views that don't exist. When this clause is given, a `NOTE` is generated for each nonexistent view.

## **Syntax**

```
> DROP VIEW [IF EXISTS]
    view_name [, view_name] ...
```

## **Examples**

```sql
> CREATE TABLE t1(c1 INT PRIMARY KEY, c2 INT);
> CREATE VIEW v1 AS SELECT * FROM t1;
> DROP VIEW v1;
```
