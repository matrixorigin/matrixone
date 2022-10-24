# **REVOKE**

## **Description**

Removes one or more privileges on a securable object from a role. The privileges that can be revoked are object-specific.

## **Syntax**

```
> REVOKE [IF EXISTS]
    priv_type [(column_list)]
      [, priv_type [(column_list)]] ...
    ON object_type priv_level
```

## **Examples**

```sql
> drop role if exists rolex;
> create role rolex;
> drop user if exists userx;
> create user userx identified by '111';
> grant public to userx;
> revoke public from userx;
```
