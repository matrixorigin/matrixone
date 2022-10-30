# **CREATE USER**

## **语法说明**

在系统中创建一个新的用户。

## **语法结构**

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

### 语法说明

#### auth_option

指定用户的默认用户名与授权方式，并返回随机密码。

#### password_option

修改密码过期时间，重启时间，以及锁定时间段。

## **示例**

```sql
> create user userx identified by '111';
Query OK, 0 rows affected (0.04 sec)
```
