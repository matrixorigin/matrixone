# **FROM_UNIXTIME()**

## **函数说明**

``FROM_UNIXTIME()`` 函数把内部 UNIX 时间戳值转换为普通格式的日期时间值，以 *YYYY-MM-DD HH:MM:SS* 或 *YYYYMMDDHHMMSS* 格式来显示。与 ``UNIX_TIMESTAMP ()`` 函数互为反函数。

## **函数语法**

```
> FROM_UNIXTIME(unix_timestamp[,format])
```

## **参数释义**

|  参数   | 说明  |
|  ----  | ----  |
| format  | 可选参数。 表示返回值格式的格式字符串。<br> 如果省略 format，则返回一个 ``DATETIME`` 值。 <br>如果 format 为空，则返回 ``NULL``。<br>format用于格式化结果，其方式与 `DATE_FORMAT()` 函数使用的格式字符串相同。如果 format 已存在指定格式，则返回值为 ``VARCHAR``。|
|unix_timestamp|必要参数。 时间戳，可以用数据库里的存储时间数据的字段。<br>如果 unix_timestamp 为空，则返回 ``NULL``。 <br>如果 unix_timestamp 是一个整数，则 ``DATETIME`` 的小数秒精度为零。当 unix_timestamp 是十进制值时，``DATETIME``的小数秒精度与十进制值的精度相同，最多可达6秒。当 ``unix_timestamp``是浮点数时，``datetime`` 的分秒精度为6。 |

## **示例**

```sql
> SELECT FROM_UNIXTIME(1447430881);
+---------------------------+
| from_unixtime(1447430881) |
+---------------------------+
| 2015-11-13 06:08:01       |
+---------------------------+
1 row in set (0.00 sec)

> SELECT FROM_UNIXTIME(1447430881,  '%Y %D %M %h:%i:%s %x');
+-------------------------------------------------+
| from_unixtime(1447430881, %Y %D %M %h:%i:%s %x) |
+-------------------------------------------------+
| 2015 13th November 06:08:01 2015                |
+-------------------------------------------------+
1 row in set (0.01 sec)
```

## **限制**

目前date格式只支持 `yyyy-mm-dd` 和 `yyyymmdd` 的数据格式。  
