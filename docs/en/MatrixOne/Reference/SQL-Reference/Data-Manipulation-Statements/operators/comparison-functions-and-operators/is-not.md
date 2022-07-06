# **IS NOT**

## **Description**

The `IS NOT` tests a value against a boolean value, where boolean_value can be TRUE, FALSE, or UNKNOWN.

## **Syntax**

```
> IS NOT boolean_value
```

## **Examples**

```sql
> SELECT 1 IS NOT TRUE, 0 IS NOT FALSE, NULL IS NOT UNKNOWN;
+-----------+------------+----------+
| 1 != true | 0 != false | null !=  |
+-----------+------------+----------+
| false     | false      | NULL     |
+-----------+------------+----------+
1 row in set (0.01 sec)
```
