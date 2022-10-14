# **CREATE ROLE**

## **Description**

Create a new role in the system.

After creating roles, you can grant object privileges to the role and then grant the role to other roles or individual users to enable access control security for objects in the system.

## **Syntax**

```
> CREATE ROLE [IF NOT EXISTS] role [, role ] ...
```

## **Examples**

```sql
> create role rolex;
Query OK, 0 rows affected (0.02 sec)
```
