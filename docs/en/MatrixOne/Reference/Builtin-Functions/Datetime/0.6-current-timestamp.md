# **CURRENT_TIMESTAMP()**

## **Description**

`CURRENT_TIMESTAMP` and `CURRENT_TIMESTAMP()` are synonyms for `NOW()`.

Returns the current date and time as a value in `YYYY-MM-DD hh:mm:ss` or `YYYYMMDDhhmmss` format, depending on whether the function is used in string or numeric context. The value is expressed in the session time zone.

## **Syntax**

```
> CURRENT_TIMESTAMP([fsp])
```

## **Arguments**

|  Arguments   | Description  |
|  ----  | ----  |
| fsp | Optional. If the `fsp` argument is given to specify a fractional seconds precision from 0 to 6, the return value includes a fractional seconds part of that many digits.  |

## **Examples**

```sql
> SELECT CURRENT_TIMESTAMP();
+----------------------------+
| current_timestamp()        |
+----------------------------+
| 2022-09-21 11:46:44.153777 |
+----------------------------+
1 row in set (0.00 sec)

> SELECT NOW();
+----------------------------+
| now()                      |
+----------------------------+
| 2022-09-21 12:56:36.915961 |
+----------------------------+
1 row in set (0.01 sec)

> create table t1 (a int primary key, b int, c int, d timestamp default current_timestamp);
> insert into t1 select 1,1,1,now();
> insert into t1 select 2,0,0,null;
> select a,b,c,year(d) from t1;
+------+------+------+---------+
| a    | b    | c    | year(d) |
+------+------+------+---------+
|    1 |    1 |    1 |    2022 |
|    2 |    0 |    0 |    NULL |
+------+------+------+---------+
2 rows in set (0.01 sec)
```

## **Constraints**

Operator `+` or `-` is not supported for using with `CURRENT_TIMESTAMP` now.
