# **CREATE ROLE**

## **语法说明**

在系统中创建一个新角色。

创建角色后，可以将权限授予该角色，然后再将该角色授予其他角色或单个用户。

## **语法结构**

```
> CREATE ROLE [IF NOT EXISTS] role [, role ] ...
```

## **示例**

```sql
> create role rolex;
Query OK, 0 rows affected (0.02 sec)
```
