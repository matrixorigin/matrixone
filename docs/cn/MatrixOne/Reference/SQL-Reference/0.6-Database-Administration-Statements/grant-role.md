# **GRANT ROLE**

## **语法说明**

将角色分配给用户或其他角色：

- 将一个角色授予另一个角色会在角色之间创建层级关系。

- 将角色授予给用户后，用户可以执行该角色所允许的所有操作(通过授予该角色的访问权限)。

## **语法结构**

```
> GRANT role [, role] ...
    TO user_or_role [, user_or_role] ...
    [WITH GRANT OPTION]
```

## **示例**

```sql
> create role role_r1,role_r2,role_r3;
> create user role_u1 identified by '111', role_u2 identified by '111', role_u3 identified by '111';
> grant role_r1 to role_u1;
Query OK, 0 rows affected (0.02 sec)
```
