# **REVOKE**

## **语法说明**

将某个角色上被赋予的权限收回。

## **语法结构**

```
> REVOKE [IF EXISTS]
    priv_type [(column_list)]
      [, priv_type [(column_list)]] ...
    ON object_type priv_level
```

## **示例**

```sql
> drop role if exists rolex;
> create role rolex;
> drop user if exists userx;
> create user userx identified by '111';
> grant public to userx;
> revoke public from userx;
```
