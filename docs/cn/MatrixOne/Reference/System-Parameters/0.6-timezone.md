# Time Zone Support

The time zone in MatrixOne is decided by the global `time_zone` system variable. The default value of time_zone is SYSTEM.

You can use the following statement to set the global server time_zone value at runtime:

```
> SET GLOBAL time_zone = timezone;
```

You can use the following statement to view the current values of the global, client-specific and system time zones:

```
> SELECT @@global.time_zone, @@session.time_zone, @@global.system_time_zone;
+-------------+-------------+--------------------+
| @@time_zone | @@time_zone | @@system_time_zone |
+-------------+-------------+--------------------+
| timezone    | +08:00      |                    |
+-------------+-------------+--------------------+
1 row in set (0.00 sec)
```

To set the format of the value of the time_zone:

- The value `SYSTEM` indicates that the time zone should be the same as the system time zone.

- The value can be given as a string indicating an offset from UTC, such as `+10:00` or `-06:00`.

- The value can be given as a named time zone, such as `Europe/Helsinki`, `US/Eastern`, or `MET`.

The current session time zone setting affects the display and storage of time values that are zone-sensitive. This includes the values displayed by functions such as `NOW()`<!-- or CURTIME()-->.

!!! note
    Only the values of the Timestamp data type is affected by time zone. This is because the Timestamp data type uses the literal value + time zone information. Other data types, such as Datetime/Date/Time, do not have time zone information, thus their values are not affected by the changes of time zone.

```sql
> SELECT @@global.time_zone, @@session.time_zone, @@global.system_time_zone;
+-------------+-------------+--------------------+
| @@time_zone | @@time_zone | @@system_time_zone |
+-------------+-------------+--------------------+
| SYSTEM      | SYSTEM      |                    |
+-------------+-------------+--------------------+
1 row in set (0.01 sec)
```

```sql
> create table t (ts timestamp, dt datetime);
Query OK, 0 rows affected (0.02 sec)

mysql> set @@time_zone = 'UTC';
Query OK, 0 rows affected (0.00 sec)

mysql> insert into t values ('2017-09-30 11:11:11', '2017-09-30 11:11:11');
Query OK, 1 row affected (0.02 sec)

mysql> set @@time_zone = '+8:00';
ERROR 20101 (HY000): internal error: incorrect timezone +8:00
mysql> set @@time_zone = '+08:00';
Query OK, 0 rows affected (0.00 sec)

mysql> select * from t;
+---------------------+---------------------+
| ts                  | dt                  |
+---------------------+---------------------+
| 2017-09-30 19:11:11 | 2017-09-30 11:11:11 |
+---------------------+---------------------+
1 row in set (0.00 sec)
```

In this example, no matter how you adjust the value of the time zone, the value of the Datetime data type is not affected. But the displayed value of the Timestamp data type changes if the time zone information changes. In fact, the value that is stored in the storage does not change, it's just displayed differently according to different time zone setting.

!!! note
    Time zone is involved during the conversion of the value of Timestamp and Datetime, which is handled based on the current time_zone.

## Changing MatrixOne Time Zone

1. View the current time and time zone:

```sql
> select now();
+----------------------------+
| now()                      |
+----------------------------+
| 2022-10-14 18:38:27.876181 |
+----------------------------+
1 row in set (0.00 sec)


> show variables like "%time_zone%";
+------------------+--------+
| Variable_name    | Value  |
+------------------+--------+
| system_time_zone |        |
| time_zone        | +08:00 |
+------------------+--------+
2 rows in set (0.00 sec)
```

- `time_zone` indicates that MatrixOne uses the system time zone.

- `system_time_zone` indicates that MatrixOne uses the CST time zone.

2. modify the time zone:

```
set global time_zone = '+08:00';
set time_zone = '+08:00';
```

- `set global time_zone = '+08:00';`: Change the global time zone to Beijing time if we are in the east eight zones.
- `set time_zone = '+08:00';`: Change the time zone of the current session.
