# **UTC_TIMESTAMP()**

## **Description**

Returns the current UTC date and time as a value in `YYYY-MM-DD hh:mm:ss` or `YYYYMMDDhhmmss` format, depending on whether the function is used in string or numeric context.

## **Syntax**

```
> UTC_TIMESTAMP, UTC_TIMESTAMP([fsp])
```

### **Arguments**

|  Arguments   | Description  |
|  ----  | ----  |
| fsp  | Optional. If the `fsp` argument is given to specify a fractional seconds precision from 0 to 6, the return value includes a fractional seconds part of that many digits. |

## **Examples**

- Example 1:

```sql
> SELECT UTC_TIMESTAMP();
+---------------------+
| utc_timestamp()     |
+---------------------+
| 2022-09-16 03:37:40 |
+---------------------+
1 row in set (0.01 sec)

> select unix_timestamp(utc_timestamp());
+---------------------------------+
| unix_timestamp(utc_timestamp()) |
+---------------------------------+
|                      1663282842 |
+---------------------------------+
1 row in set (0.02 sec)
```

- Example 2:

```sql
> create table t1 (ts timestamp);
> set time_zone='+00:00';
> select unix_timestamp(utc_timestamp())-unix_timestamp(utc_timestamp());
+-------------------------------------------------------------------+
| unix_timestamp(utc_timestamp()) - unix_timestamp(utc_timestamp()) |
+-------------------------------------------------------------------+
|                                                                 0 |
+-------------------------------------------------------------------+
1 row in set (0.00 sec)

> insert into t1 (ts) values ('2003-03-30 02:30:00');
> set time_zone='+10:30';
> select unix_timestamp(utc_timestamp())-unix_timestamp(utc_timestamp());
+-------------------------------------------------------------------+
| unix_timestamp(utc_timestamp()) - unix_timestamp(utc_timestamp()) |
+-------------------------------------------------------------------+
|                                                                 0 |
+-------------------------------------------------------------------+
1 row in set (0.01 sec)

> insert into t1 (ts) values ('2003-03-30 02:30:00');
> set time_zone='-10:00';
> select unix_timestamp(utc_timestamp())-unix_timestamp(current_timestamp());
+-----------------------------------------------------------------------+
| unix_timestamp(utc_timestamp()) - unix_timestamp(current_timestamp()) |
+-----------------------------------------------------------------------+
|                                                                 36000 |
+-----------------------------------------------------------------------+
1 row in set (0.00 sec)

> insert into t1 (ts) values ('2003-03-30 02:30:00');
> select * from t1;
+---------------------+
| ts                  |
+---------------------+
| 2003-03-29 16:30:00 |
| 2003-03-29 06:00:00 |
| 2003-03-30 02:30:00 |
+---------------------+
3 rows in set (0.00 sec)
```

- Example 3:

```sql
> CREATE TABLE t1 (a TIMESTAMP);
> INSERT INTO t1 select (utc_timestamp());
> INSERT INTO t1 select (utc_timestamp());
> INSERT INTO t1 select (utc_timestamp());
> SELECT year(a) FROM t1 WHERE a > '2008-01-01';
+---------+
| year(a) |
+---------+
|    2022 |
|    2022 |
|    2022 |
+---------+
3 rows in set (0.04 sec)

> DROP TABLE if exists t1;
> create table t1 (a int primary key, b int, c int, d timestamp);
> insert into t1 select 1,1,1,utc_timestamp();
> insert into t1 select 2,0,0,null;
> select a,b,c,year(d) from t1;
+------+------+------+---------+
| a    | b    | c    | year(d) |
+------+------+------+---------+
|    1 |    1 |    1 |    2022 |
|    2 |    0 |    0 |    NULL |
+------+------+------+---------+
2 rows in set (0.01 sec)

> DROP TABLE t1;
> CREATE TABLE t1 (a TIMESTAMP);
> INSERT INTO t1 select (utc_timestamp());
> INSERT INTO t1 select (utc_timestamp());
> SELECT 1 FROM t1 ORDER BY 1;
+------+
| 1    |
+------+
|    1 |
|    1 |
+------+
2 rows in set (0.01 sec)
```

## **Constraints**

Operator `+` or `-` with parameters `DATETIME BIGINT` is not supported now.
