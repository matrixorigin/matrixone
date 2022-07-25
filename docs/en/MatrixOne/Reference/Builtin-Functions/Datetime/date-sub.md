# **DATE_SUB()**

## **Description**

The ``DATE_SUB()`` function subtracts a time/date interval from a date and then returns the date. If date is ``NULL``, the function returns ``NULL``.

## **Syntax**

```
DATE_SUB(date,INTERVAL expr unit)
```

## **Arguments**

|  Arguments   | Description  |
|  ----  | ----  |
| date | Required. The date/datetime to extract the date from. |
| expr  | Required.  The expr is an expression specifying the interval value to be added or subtracted from the starting date. The expr is evaluated as a string; it may start with a - for negative intervals. |
| unit| Required. The unit is a keyword indicating the units in which the expression should be interpreted. The unit argument can have the following values:<br>MICROSECOND <br>SECOND<br>MINUTE<br>HOUR<br>DAY<br>WEEK<br>MONTH<br>QUA<br>TER<br>YEAR<br>SECOND_MICROSECOND<br>MINUTE_MICROSECOND<br>MINUTE_SECOND<br>HOUR_MICROSECOND<br>HOUR_SECOND<br>HOUR_MINUTE<br>DAY_MICROSECOND<br>DAY_SECOND<br>DAY_MINUTE<br>DAY_HOUR<br>YEAR_MONTH|

## **Examples**

```sql
> create table t2(orderid int, productname varchar(20), orderdate datetime);
> insert into t2 values ('1','Jarl','2008-11-11 13:23:44.657');
> SELECT OrderId,DATE_SUB(OrderDate,INTERVAL 5 DAY) AS SubtractDate FROM t2;
+---------+---------------------+
| orderid | subtractdate        |
+---------+---------------------+
|       1 | 2008-11-06 13:23:44 |
+---------+---------------------+
```

## **Constraints**

The date type supports only `yyyy-mm-dd` and `yyyymmdd` for now.
