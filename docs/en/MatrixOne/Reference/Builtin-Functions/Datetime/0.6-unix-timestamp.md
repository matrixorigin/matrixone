# **UNIX_TIMESTAMP()**

## **Description**

If ``UNIX_TIMESTAMP()`` is called with no date argument, it returns a Unix timestamp representing seconds since '1970-01-01 00:00:00' UTC.

If ``UNIX_TIMESTAMP()`` is called with a date argument, it returns the value of the argument as seconds since '1970-01-01 00:00:00' UTC. The server interprets date as a value in the session time zone and converts it to an internal Unix timestamp value in UTC.  

If you pass an out-of-range date to UNIX_TIMESTAMP(), it returns 0. If date is ``NULL``, it returns ``NULL``.

The return value is an integer if no argument is given or the argument does not include a fractional seconds part, or DECIMAL if an argument is given that includes a fractional seconds part.

## **Syntax**

```
> UNIX_TIMESTAMP([date])
```

### **Arguments**

|  Arguments   | Description  |
|  ----  | ----  |
| date  | Optional. The date/datetime to extract the date from. <br>The date argument may be a DATE, DATETIME, or TIMESTAMP string, or a number in YYMMDD, YYMMDDhhmmss, YYYYMMDD, or YYYYMMDDhhmmss format. If the argument includes a time part, it may optionally include a fractional seconds part. <br>When the date argument is a ``TIMESTAMP`` column, ``UNIX_TIMESTAMP()`` returns the internal timestamp value directly, with no implicit *string-to-Unix-timestamp* conversion.|

### Convert between non-UTC time zone and Unix timestamp values

If you use `UNIX_TIMESTAMP()` and `FROM_UNIXTIME()` to convert between values in a non-UTC time zone and Unix timestamp values, the conversion is lossy because the mapping is not one-to-one in both directions. For example, due to conventions for local time zone changes such as Daylight Saving Time (DST), it is possible for `UNIX_TIMESTAMP()` to map two values that are distinct in a non-UTC time zone to the same Unix timestamp value. `FROM_UNIXTIME()` maps that value back to only one of the original values. Here is an example, using values that are distinct in the MET time zone:

```sql
> SET time_zone = 'MET';
> SELECT UNIX_TIMESTAMP('2005-03-27 03:00:00');
+-------------------------------------+
| unix_timestamp(2005-03-27 03:00:00) |
+-------------------------------------+
|                          1111885200 |
+-------------------------------------+

> SELECT UNIX_TIMESTAMP('2005-03-27 02:00:00');
+-------------------------------------+
| unix_timestamp(2005-03-27 02:00:00) |
+-------------------------------------+
|                          1111885200 |
+-------------------------------------+

> SELECT FROM_UNIXTIME(1111885200);
+---------------------------+
| from_unixtime(1111885200) |
+---------------------------+
| 2005-03-27 03:00:00       |
+---------------------------+
```

## **Examples**

```sql
> SELECT UNIX_TIMESTAMP("2016-07-11");
+----------------------------+
| unix_timestamp(2016-07-11) |
+----------------------------+
|                 1468195200 |
+----------------------------+

> SELECT UNIX_TIMESTAMP('2015-11-13 10:20:19');
+-------------------------------------+
| unix_timestamp(2015-11-13 10:20:19) |
+-------------------------------------+
|                          1447381219 |
+-------------------------------------+
1 row in set (0.03 sec)

> SELECT UNIX_TIMESTAMP('2015-11-13 10:20:19.012');
+-----------------------------------------+
| unix_timestamp(2015-11-13 10:20:19.012) |
+-----------------------------------------+
|                              1447381219 |
+-----------------------------------------+
```

## **Constraints**

The date type supports only `yyyy-mm-dd` and `yyyymmdd` for now.
