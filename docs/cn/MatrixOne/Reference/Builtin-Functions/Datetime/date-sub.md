# **DATE_SUB()**

## **函数说明**

``DATE_SUB()``都用于执行日期运算：``DATE_SUB()`` 函数从日期减去指定的时间间隔。如果 ``date`` 为``NULL``，函数返回 ``NULL``。

## **函数语法**

```
DATE_SUB(date,INTERVAL expr unit)
```

## **参数释义**

|  参数   | 说明 |
|  ----  | ----  |
| date| 必要参数。 date 参数是合法的日期表达式。 |
| expr  | 必要参数。  expr 参数是需要添加进 date 的时间间隔，如果 expr 为负数，那么可以以“-”开头。 |
| unit| 必要参数。 unit 参数可以是下列值：<br>MICROSECOND <br>SECOND<br>MINUTE<br>HOUR<br>DAY<br>WEEK<br>MONTH<br>QUA<br>TER<br>YEAR<br>SECOND_MICROSECOND<br>MINUTE_MICROSECOND<br>MINUTE_SECOND<br>HOUR_MICROSECOND<br>HOUR_SECOND<br>HOUR_MINUTE<br>DAY_MICROSECOND<br>DAY_SECOND<br>DAY_MINUTE<br>DAY_HOUR<br>YEAR_MONTH|

## **示例**

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

## **限制**

目前date格式只支持 `yyyy-mm-dd` 和 `yyyymmdd` 的数据格式。
