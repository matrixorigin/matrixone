# **DAY()**

## **Description**

Returns the day of the month for date, in the range 1 to 31, or 0 for dates such as `0000-00-00` or `2008-00-00` that have a zero day part. Returns NULL if date is `NULL`.

## **Syntax**

```
> DAY(date)
```

## **Arguments**

|  Arguments   | Description  |
|  ----  | ----  |
| date | Required. The date/datetime to extract the date from. |

## **Examples**

```sql
> select day('2007-02-03');
+-----------------+
| day(2007-02-03) |
+-----------------+
|               3 |
+-----------------+
1 row in set (0.01 sec)

> CREATE TABLE t3(c1 TIMESTAMP NOT NULL);
> INSERT INTO t3 VALUES('2000-01-01');
> INSERT INTO t3 VALUES('1999-12-31');
> INSERT INTO t3 VALUES('2000-01-01');
> INSERT INTO t3 VALUES('2006-12-25');
> INSERT INTO t3 VALUES('2008-02-29');
> select day(c1) from t3;
+---------+
| day(c1) |
+---------+
|       1 |
|      31 |
|       1 |
|      25 |
|      29 |
+---------+
5 rows in set (0.01 sec)
```
