# **GRANT <privileges>**

## **语法说明**

将权限授权给角色，角色可以拥有对对象上的一个或多个访问特权。

## **语法结构**

```
> GRANT
    priv_type [(column_list)]
      [, priv_type [(column_list)]] ...
    ON object_type priv_level
    TO role [, role] ...
    [WITH GRANT OPTION]

object_type: {
    TABLE
  | DATABASE  
  | FUNCTION
  | ACCOUNT
}

priv_level: {
    *
  | *.*
  | db_name
  | db_name.*
  | db_name.tbl_nam e
  | tbl_name
  | db_name.routine_name
}
```

## **示例**

```sql
> drop role if exists rolex;
> create role rolex;
> drop user if exists userx;
> create user userx identified by '111';
> grant public to userx;
Query OK, 0 rows affected (0.03 sec)
```
