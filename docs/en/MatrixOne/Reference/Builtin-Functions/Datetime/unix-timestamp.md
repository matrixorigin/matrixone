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

## **Arguments**

|  Arguments   | Description  |
|  ----  | ----  |
| date  | Optional. The date/datetime to extract the date from. <br>The date argument may be a DATE, DATETIME, or TIMESTAMP string, or a number in YYMMDD, YYMMDDhhmmss, YYYYMMDD, or YYYYMMDDhhmmss format. If the argument includes a time part, it may optionally include a fractional seconds part. <br>When the date argument is a ``TIMESTAMP`` column, ``UNIX_TIMESTAMP()`` returns the internal timestamp value directly, with no implicit *string-to-Unix-timestamp* conversion.|

## **Examples**

```sql
> SELECT UNIX_TIMESTAMP("2016-07-11");
+----------------------------+
| unix_timestamp(2016-07-11) |
+----------------------------+
|                 1468195200 |
+----------------------------+
```

## **Constraints**

The date type supports only `yyyy-mm-dd` and `yyyymmdd` for now.
