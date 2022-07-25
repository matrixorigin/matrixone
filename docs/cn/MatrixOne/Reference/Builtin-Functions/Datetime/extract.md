# **EXTRACT()**

## **函数说明**

``EXTRACT()`` 函数是从日期中提取部分内容。如果日期是 ``NULL`` 则返回 NULL。

## **函数语法**

```
> EXTRACT(unit FROM date)
```

## **参数释义**

|  参数   | 说明 |
|  ----  | ----  |
| date| 必要参数。 date 参数是合法的日期表达式。 |
| unit| 必要参数。 unit 参数可以是下列值：<br>MICROSECOND <br>SECOND<br>MINUTE<br>HOUR<br>DAY<br>WEEK<br>MONTH<br>QUA<br>TER<br>YEAR<br>SECOND_MICROSECOND<br>MINUTE_MICROSECOND<br>MINUTE_SECOND<br>HOUR_MICROSECOND<br>HOUR_SECOND<br>HOUR_MINUTE<br>DAY_MICROSECOND<br>DAY_SECOND<br>DAY_MINUTE<br>DAY_HOUR<br>YEAR_MONTH|

## **示例**

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

## **限制**

目前date格式只支持 `yyyy-mm-dd` 和 `yyyymmdd` 的数据格式。  
