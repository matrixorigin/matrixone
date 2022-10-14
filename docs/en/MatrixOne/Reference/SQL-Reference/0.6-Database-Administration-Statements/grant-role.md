# **GRANT ROLE**

## **Description**

Assigns a role to a user or another role:

- Granting a role to another role creates a “parent-child” relationship between the roles (also referred to as a role hierarchy).

- Granting a role to a user enables the user to perform all operations allowed by the role (through the access privileges granted to the role).

## **Syntax**

```
> GRANT role [, role] ...
    TO user_or_role [, user_or_role] ...
    [WITH GRANT OPTION]
```

## **Examples**

```sql
> create role role_r1,role_r2,role_r3;
> create user role_u1 identified by '111', role_u2 identified by '111', role_u3 identified by '111';
> grant role_r1 to role_u1;
Query OK, 0 rows affected (0.02 sec)
```
