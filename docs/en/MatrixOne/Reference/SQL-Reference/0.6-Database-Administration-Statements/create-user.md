# **CREATE USER**

## **Description**

Creates a new user in the system.

## **Syntax**

```
> CREATE USER [IF NOT EXISTS]
    user auth_option [, user auth_option] ...
    [DEFAULT ROLE role]  
    [COMMENT 'comment_string' | ATTRIBUTE 'json_object']
auth_option: {
    IDENTIFIED BY 'auth_string'
}

password_option: {
    PASSWORD EXPIRE [DEFAULT | NEVER | INTERVAL N DAY]
  | PASSWORD HISTORY {DEFAULT | N}
  | PASSWORD REUSE INTERVAL {DEFAULT | N DAY}
  | PASSWORD REQUIRE CURRENT [DEFAULT | OPTIONAL]
  | FAILED_LOGIN_ATTEMPTS N
  | PASSWORD_LOCK_TIME {N | UNBOUNDED}
}
```

### Explanations

#### auth_option

Specifies the default user name and authorization mode of the tenant. Results are returned for random passwords.

#### password_option

Change the password expiration time, reuse times, and lock duration.

## **Examples**

```sql
> create user userx identified by '111';
Query OK, 0 rows affected (0.04 sec)
```
