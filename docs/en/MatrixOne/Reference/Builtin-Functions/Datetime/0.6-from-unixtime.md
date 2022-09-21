# **FROM_UNIXTIME()**

## **Description**

The ``FROM_UNIXTIME()`` function returns a representation of ``unix_timestamp`` as a datetime or character string value. The value returned is expressed using the session time zone. For example, the return value is in ‘YYYYY-MM-DD HH:MM:SS’ format or YYYYMMDDHHMMSS. ``unix_timestamp`` is an internal timestamp value representing seconds since *1970-01-01 00:00:00* UTC, such as produced by the ``UNIX_TIMESTAMP()`` function.

## **Syntax**

```
> FROM_UNIXTIME(unix_timestamp[,format])
```

## **Arguments**

|  Arguments   | Description  |
|  ----  | ----  |
| format  | Optional.  A format string indicating the format of the return value.<br> If the format is omitted, this function returns a DATETIME value. <br>If the format is ``NULL``, this function returns ``NULL``.<br>format is used to format the result in the same way as the format string used for the `DATE_FORMAT()` function. If format is supplied, the value returned is a `VARCHAR`.|
|unix_timestamp|Required. <br>If the unix_timestamp is ``NULL``, this function returns ``NULL``. <br>If the unix_timestamp is an *integer*, the fractional seconds precision of the ``DATETIME`` is zero. When ``unix_timestamp`` is a *decimal* value, the fractional seconds precision of the ``DATETIME`` is the same as the precision of the *decimal* value, up to a maximum of 6. When unix_timestamp is a floating point number, the fractional seconds precision of the datetime is 6. |

## **Examples**

```sql
> SELECT FROM_UNIXTIME(1447430881);
+---------------------------+
| from_unixtime(1447430881) |
+---------------------------+
| 2015-11-13 06:08:01       |
+---------------------------+
1 row in set (0.00 sec)

> SELECT FROM_UNIXTIME(1447430881,  '%Y %D %M %h:%i:%s %x');
+-------------------------------------------------+
| from_unixtime(1447430881, %Y %D %M %h:%i:%s %x) |
+-------------------------------------------------+
| 2015 13th November 06:08:01 2015                |
+-------------------------------------------------+
1 row in set (0.01 sec)
```

## **Constraints**

The date type supports only `yyyy-mm-dd` and `yyyymmdd` for now.
