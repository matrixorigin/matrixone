# **EXTRACT()**

## **Description**

The ``EXTRACT()`` function uses the same kinds of unit specifiers as ``DATE_ADD()`` or ``DATE_SUB()``, but extracts parts from the date rather than performing date arithmetic. Returns NULL if date is ``NULL``.

## **Syntax**

```
> EXTRACT(unit FROM date)
```

## **Arguments**

|  Arguments   | Description  |
|  ----  | ----  |
| date  | Required.  The date/datetime to extract the date from. |
| unit| Required. The unit argument can have the following values:<br>MICROSECOND <br>SECOND<br>MINUTE<br>HOUR<br>DAY<br>WEEK<br>MONTH<br>QUA<br>TER<br>YEAR<br>SECOND_MICROSECOND<br>MINUTE_MICROSECOND<br>MINUTE_SECOND<br>HOUR_MICROSECOND<br>HOUR_SECOND<br>HOUR_MINUTE<br>DAY_MICROSECOND<br>DAY_SECOND<br>DAY_MINUTE<br>DAY_HOUR<br>YEAR_MONTH|

## **Examples**

```sql
> create table t2(orderid int, productname varchar(20), orderdate datetime);
> insert into t2 values ('1','Jarl','2008-11-11 13:23:44.657');
> SELECT EXTRACT(YEAR FROM OrderDate) AS OrderYear, EXTRACT(MONTH FROM OrderDate) AS OrderMonth   FROM t2 WHERE OrderId=1;
+-----------+------------+
| orderyear | ordermonth |
+-----------+------------+
| 2008      | 11         |
+-----------+------------+
```

## **Constraints**

The date type supports only `yyyy-mm-dd` and `yyyymmdd` for now.
