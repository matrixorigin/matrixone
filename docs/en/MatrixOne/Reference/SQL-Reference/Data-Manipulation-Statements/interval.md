# **INTERVAL**

## **Description**

The `INTERVAL` values are used mainly for date and time calculations.

## **Syntax**

```
> INTERVAL (expr,unit)
```

## **Examples**

```sql
> select INTERVAL 1 DAY + "1997-12-31";
+-------------------------------+
| interval(1, day) + 1997-12-31 |
+-------------------------------+
| 1998-01-01                    |
+-------------------------------+
1 row in set (0.01 sec)
```
