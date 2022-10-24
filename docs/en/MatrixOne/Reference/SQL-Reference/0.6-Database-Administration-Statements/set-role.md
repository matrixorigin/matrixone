# **SET ROLE**

## **Description**

Specifies the active/current primary role for the session. The currently-active primary role sets the context that determines whether the current user has the necessary privileges to execute `CREATE <object>` statements or perform any other SQL action.

Note that authorization to perform any SQL action other than creating objects can be provided by secondary roles.

## **Syntax**

```
> SET SECONDARY ROLE {
    NONE
  | ALL  
}
SET ROLE role
```

### Explanations

#### SET SECONDARY ROLE ALL

The union of all roles of the user.

#### SET SECONDARY ROLE NONE

Kicking all roles except the PRIMARY ROLE from the current session.

#### SET ROLE role

Switching the current ROLE to a new role.

## **Examples**

```sql
