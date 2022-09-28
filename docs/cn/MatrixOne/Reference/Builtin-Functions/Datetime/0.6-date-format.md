# **DATE_FORMAT()**

## **函数说明**

根据格式字符串格式化日期值。如果任一参数为 `NULL`，则函数返回 `NULL`。

`DATE_FORMAT()` 返回一个字符串，其中包含由 `character_set_connection` 和 `collat​​ion_connection` 给出的字符集和排序规则，以便它可以返回包含非 ASCII 字符的月份和工作日名称。

## **函数语法**

```
> DATE_FORMAT(date,format)
```

## **参数释义**

|  参数   | 说明 |
|  ----  | ----  |
| date  | 必要参数。 date 参数是合法的日期表达式。  |
| format| 必要参数。 Required. format 可用的说明符可以参加下表详情。|

### Format 说明符

!!! info
    下表中显示的说明符可用于格式字符串。在格式说明符字符之前需要加 `%` 字符。说明符也适用于函数 `UNIX_TIMESTAMP()`。

| **说明符** | **描述** |
| --- | --- |
| %a | Abbreviated weekday name (Sun..Sat) |
| %b | Abbreviated month name (Jan..Dec) |
| %c | Month, numeric (0..12) |
| %D | Day of the month with English suffix (0th, 1st, 2nd, 3rd, …) |
| %d | Day of the month, numeric (00..31) |
| %e | Day of the month, numeric (0..31) |
| %f | Microseconds (000000..999999) |
| %H | Hour (00..23) |
| %h | Hour (01..12) |
| %I | Hour (01..12) |
| %i | Minutes, numeric (00..59) |
| %j | Day of year (001..366) |
| %k | Hour (0..23) |
| %l | Hour (1..12) |
| %M | Month name (January..December) |
| %m | Month, numeric (00..12) |
| %p | AM or PM |
| %r | Time, 12-hour (hh:mm:ss followed by AM or PM) |
| %S | Seconds (00..59) |
| %s | Seconds (00..59) |
| %T | Time, 24-hour (hh:mm:ss) |
| %U | Week (00..53), where Sunday is the first day of the week; WEEK() mode 0 |
| %u | Week (00..53), where Monday is the first day of the week; WEEK() mode 1 |
| %V | Week (01..53), where Sunday is the first day of the week; WEEK() mode 2; used with %X |
| %v | Week (01..53), where Monday is the first day of the week; WEEK() mode 3; used with %x |
| %W | Weekday name (Sunday..Saturday) |
| %w | Day of the week (0=Sunday..6=Saturday) |
| %X | Year for the week where Sunday is the first day of the week, numeric, four digits; used with %V |
| %x | Year for the week, where Monday is the first day of the week, numeric, four digits; used with %v |
| %Y | Year, numeric, four digits |
| %y | Year, numeric (two digits) |
| %% | A literal % character |
| %x | x, for any “x” not listed above |

## **示例**

```sql
> SELECT DATE_FORMAT('2009-10-04 22:23:00', '%W %M %Y');
+--------------------------------------------+
| date_format(2009-10-04 22:23:00, %W %M %Y) |
+--------------------------------------------+
| Sunday October 2009                        |
+--------------------------------------------+
1 row in set (0.01 sec)

> SELECT DATE_FORMAT('2007-10-04 22:23:00', '%H:%i:%s');
+--------------------------------------------+
| date_format(2007-10-04 22:23:00, %H:%i:%s) |
+--------------------------------------------+
| 22:23:00                                   |
+--------------------------------------------+
1 row in set (0.02 sec)

> SELECT Date_format('1900-10-04 22:23:00', '%D %y %a %d %m %b %j');
+--------------------------------------------------------+
| date_format(1900-10-04 22:23:00, %D %y %a %d %m %b %j) |
+--------------------------------------------------------+
| 4th 00 Thu 04 10 Oct 277                               |
+--------------------------------------------------------+
1 row in set (0.01 sec)

> SELECT DATE_FORMAT('1997-10-04 22:23:00', '%H %k %I %r %T %S %w');
+--------------------------------------------------------+
| date_format(1997-10-04 22:23:00, %H %k %I %r %T %S %w) |
+--------------------------------------------------------+
| 22 22 10 10:23:00 PM 22:23:00 00 6                     |
+--------------------------------------------------------+
1 row in set (0.01 sec)

> SELECT DATE_FORMAT('1999-01-01', '%X %V');
+--------------------------------+
| date_format(1999-01-01, %X %V) |
+--------------------------------+
| 1998 52                        |
+--------------------------------+
1 row in set (0.00 sec)
```

<!--SELECT DATE_FORMAT('2006-06-00', '%d');
ERROR 1105 (HY000): Can't cast '2006-06-00' from VARCHAR type to DATETIME type.-->

```sql
> CREATE TABLE t2 (f1 DATETIME);
> INSERT INTO t2 (f1) VALUES ('2005-01-01');
> INSERT INTO t2 (f1) VALUES ('2005-02-01');
> SELECT Date_format(f1, "%m") AS d1,
         Date_format(f1, "%m") AS d2
  FROM   t2
  ORDER  BY Date_format(f1, "%m");
+------+----------+
| d1   | d2       |
+------+----------+
| 02   | February |
| 01   | January  |
+------+----------+

> CREATE TABLE t5 (a int, b date);
> INSERT INTO t5
  VALUES    (1,
             '2000-02-05'),
            (2,
             '2000-10-08'),
            (3,
             '2005-01-03'),
            (4,
             '2007-09-01'),
            (5,
             '2022-01-01');
> SELECT * FROM   t5
  WHERE  b = Date_format('20000205', '%Y-%m-%d');
+------+------------+
| a    | b          |
+------+------------+
|    1 | 2000-02-05 |
+------+------------+
1 row in set (0.01 sec)

> SELECT * FROM   t5
  WHERE  b != Date_format('20000205', '%Y-%m-%d');
+------+------------+
| a    | b          |
+------+------------+
|    2 | 2000-10-08 |
|    3 | 2005-01-03 |
|    4 | 2007-09-01 |
|    5 | 2022-01-01 |
+------+------------+
4 rows in set (0.01 sec)

> SELECT DATE_FORMAT("2009-01-01",'%W %d %M %Y') as valid_date;
+--------------------------+
| valid_date               |
+--------------------------+
| Thursday 01 January 2009 |
+--------------------------+
1 row in set (0.00 sec)
```

## **限制**

目前date格式只支持 `yyyy-mm-dd` 和 `yyyymmdd` 的数据格式。
