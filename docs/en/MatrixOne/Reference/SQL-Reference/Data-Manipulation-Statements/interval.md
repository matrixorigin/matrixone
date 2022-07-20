# **INTERVAL**

## **Description**

- The `INTERVAL` values are used mainly for date and time calculations. The `INTERVAL` in expressions represents a temporal interval.

- Temporal intervals are used for certain functions, such as `DATE_ADD()` and `DATE_SUB()`.

- Temporal arithmetic also can be performed in expressions using INTERVAL together with the `+` or `-` operator:

```
date + INTERVAL expr unit
date - INTERVAL expr unit
```

  + INTERVAL expr unit is permitted on either side of the `+` operator if the expression on the other side is a date or datetime value.
  + For the `-` operator, INTERVAL expr unit is permitted only on the right side, because it makes no sense to subtract a date or datetime value from an interval.

## **Syntax**

```
> INTERVAL (expr,unit)
```

## **Arguments**

|  Arguments   | Description  |
|  ----  | ----  |
|expr| represents a quantity.|
|unit| the unit for interpreting the quantity; it is a specifier such as HOUR, DAY, or WEEK.|

Note: The `INTERVAL` keyword and the `unit` specifier are not case-sensitive.

- **Temporal Interval Expression and Unit Arguments**

| **_unit_ Value** | **Expected _expr_ Format** |
| --- | --- |
| MICROSECOND | MICROSECONDS |
| SECOND | SECONDS |
| MINUTE | MINUTES |
| HOUR | HOURS |
| DAY | DAYS |
| WEEK | WEEKS |
| MONTH | MONTHS |
| QUARTER | QUARTERS |
| YEAR | YEARS |
| SECOND_MICROSECOND | 'SECONDS.MICROSECONDS' |
| MINUTE_MICROSECOND | 'MINUTES:SECONDS.MICROSECONDS' |
| MINUTE_SECOND | 'MINUTES:SECONDS' |
| HOUR_MICROSECOND | 'HOURS:MINUTES:SECONDS.MICROSECONDS' |
| HOUR_SECOND | 'HOURS:MINUTES:SECONDS' |
| HOUR_MINUTE | 'HOURS:MINUTES' |
| DAY_MICROSECOND | 'DAYS HOURS:MINUTES:SECONDS.MICROSECONDS' |
| DAY_SECOND | 'DAYS HOURS:MINUTES:SECONDS' |
| DAY_MINUTE | 'DAYS HOURS:MINUTES' |
| DAY_HOUR | 'DAYS HOURS' |
| YEAR_MONTH | 'YEARS-MONTHS' |

We permits any punctuation delimiter in the expr format. Those shown in the table are the suggested delimiters.

## **Examples**

### Example 1

- Temporal intervals are used for `DATE_ADD()` and `DATE_SUB()`:

```SQL
> SELECT DATE_SUB('2018-05-01',INTERVAL 1 YEAR);
+-----------------------------------------+
| date_sub(2018-05-01, interval(1, year)) |
+-----------------------------------------+
| 2017-05-01                              |
+-----------------------------------------+
1 row in set (0.00 sec)

> SELECT DATE_ADD('2020-12-31 23:59:59', INTERVAL 1 SECOND);
+----------------------------------------------------+
| date_add(2020-12-31 23:59:59, interval(1, second)) |
+----------------------------------------------------+
| 2021-01-01 00:00:00                                |
+----------------------------------------------------+
1 row in set (0.01 sec)

> SELECT DATE_ADD('2018-12-31 23:59:59', INTERVAL 1 DAY);
+-------------------------------------------------+
| date_add(2018-12-31 23:59:59, interval(1, day)) |
+-------------------------------------------------+
| 2019-01-01 23:59:59                             |
+-------------------------------------------------+
1 row in set (0.00 sec)

> SELECT DATE_ADD('2100-12-31 23:59:59', INTERVAL '1:1' MINUTE_SECOND);
+-------------------------------------------------------------+
| date_add(2100-12-31 23:59:59, interval(1:1, minute_second)) |
+-------------------------------------------------------------+
| 2101-01-01 00:01:00                                         |
+-------------------------------------------------------------+
1 row in set (0.00 sec)

> SELECT DATE_SUB('2025-01-01 00:00:00', INTERVAL '1 1:1:1' DAY_SECOND);
+--------------------------------------------------------------+
| date_sub(2025-01-01 00:00:00, interval(1 1:1:1, day_second)) |
+--------------------------------------------------------------+
| 2024-12-30 22:58:59                                          |
+--------------------------------------------------------------+
1 row in set (0.00 sec)

> SELECT DATE_ADD('1900-01-01 00:00:00', INTERVAL '-1 10' DAY_HOUR);
+----------------------------------------------------------+
| date_add(1900-01-01 00:00:00, interval(-1 10, day_hour)) |
+----------------------------------------------------------+
| 1900-01-02 10:00:00                                      |
+----------------------------------------------------------+
1 row in set (0.00 sec)

> SELECT DATE_SUB('1998-01-02', INTERVAL 31 DAY);
+-----------------------------------------+
| date_sub(1998-01-02, interval(31, day)) |
+-----------------------------------------+
| 1997-12-02                              |
+-----------------------------------------+
1 row in set (0.00 sec)

> SELECT DATE_ADD('1992-12-31 23:59:59.000002', INTERVAL '1.999999' SECOND_MICROSECOND);
+------------------------------------------------------------------------------+
| date_add(1992-12-31 23:59:59.000002, interval(1.999999, second_microsecond)) |
+------------------------------------------------------------------------------+
| 1993-01-01 00:00:01.000001                                                   |
+------------------------------------------------------------------------------+
1 row in set (0.00 sec)
```

### Example 2

- Using INTERVAL together with the `+` or `-` operator

```sql
> SELECT '2018-12-31 23:59:59' + INTERVAL 1 SECOND;
+-------------------------------------------+
| 2018-12-31 23:59:59 + interval(1, second) |
+-------------------------------------------+
| 2019-01-01 00:00:00                       |
+-------------------------------------------+
1 row in set (0.00 sec)

> SELECT INTERVAL 1 DAY + '2018-12-31';
+-------------------------------+
| interval(1, day) + 2018-12-31 |
+-------------------------------+
| 2019-01-01                    |
+-------------------------------+
1 row in set (0.00 sec)

> SELECT '2025-01-01' - INTERVAL 1 SECOND;
+----------------------------------+
| 2025-01-01 - interval(1, second) |
+----------------------------------+
| 2024-12-31 23:59:59              |
+----------------------------------+
1 row in set (0.00 sec)
```

### Example 3

If you add to or subtract from a date value something that contains a time part, the result is automatically converted to a datetime value:

```sql
> SELECT DATE_ADD('2023-01-01', INTERVAL 1 DAY);
+----------------------------------------+
| date_add(2023-01-01, interval(1, day)) |
+----------------------------------------+
| 2023-01-02                             |
+----------------------------------------+
1 row in set (0.00 sec)

> SELECT DATE_ADD('2023-01-01', INTERVAL 1 HOUR);
+-----------------------------------------+
| date_add(2023-01-01, interval(1, hour)) |
+-----------------------------------------+
| 2023-01-01 01:00:00                     |
+-----------------------------------------+
1 row in set (0.01 sec)
```

### Example 4

If you add MONTH, YEAR_MONTH, or YEAR and the resulting date has a day that is larger than the maximum day for the new month, the day is adjusted to the maximum days in the new month:

```sql
> SELECT DATE_ADD('2019-01-30', INTERVAL 1 MONTH);
+------------------------------------------+
| date_add(2019-01-30, interval(1, month)) |
+------------------------------------------+
| 2019-02-28                               |
+------------------------------------------+
1 row in set (0.00 sec)
```

### Example 5

Date arithmetic operations require complete dates and do not work with incomplete dates such as '2016-07-00' or badly malformed dates:

```sql
> SELECT DATE_ADD('2016-07-00', INTERVAL 1 DAY);
+----------------------------------------+
| date_add(2016-07-00, interval(1, day)) |
+----------------------------------------+
| NULL                                   |
+----------------------------------------+
1 row in set (0.00 sec)

> SELECT '2005-03-32' + INTERVAL 1 MONTH;
+---------------------------------+
| 2005-03-32 + interval(1, month) |
+---------------------------------+
| NULL                            |
+---------------------------------+
1 row in set (0.00 sec)
```
