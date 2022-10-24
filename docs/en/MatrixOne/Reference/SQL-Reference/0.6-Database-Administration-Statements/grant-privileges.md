# **GRANT <privileges>**

## **Description**

Grants one or more access privileges on a securable object to a role. The privileges that can be granted are object-specific.

## **Syntax**

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

## **Examples**

```sql
> drop role if exists rolex;
> create role rolex;
> drop user if exists userx;
> create user userx identified by '111';
> grant public to userx;
Query OK, 0 rows affected (0.03 sec)
```
