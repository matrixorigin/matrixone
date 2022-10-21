# **CREATE ACCOUNT**

## **Description**

Create a new account in your organization.

## **Syntax**

```
> CREATE ACCOUNT  [IF NOT EXISTS]
account auth_option
[COMMENT 'comment_string']

auth_option: {
    ADMIN_NAME [=] 'admin_name'
    IDENTIFIED BY 'auth_string'
}

```

### Explanations

#### auth_option

Specifies the default account name and authorization mode of the tenant, `auth_string` specifies the password explicitly.

#### status_option

Specifies the initial state of a vstore after it is created.

## **Examples**

```sql
> create account tenant_test admin_name = 'root' identified by '111' open comment 'tenant_test';
Query OK, 0 rows affected (0.08 sec)
```
