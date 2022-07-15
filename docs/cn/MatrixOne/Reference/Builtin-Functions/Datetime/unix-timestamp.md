# **UNIX_TIMESTAMP()**

## **函数说明**

``UNIX_TIMESTAMP()`` 返回自 *1970-01-01 00:00:00* UTC 至当前时间的秒数。

``UNIX_TIMESTAMP(date)`` 将参数的值返回为 *1970-01-01 00:00:00* UTC 至 *date* 指定时间的秒数。

如果日期超出范围传递给 ``UNIX_TIMESTAMP()``，它将返回0。如果 ``date`` 为 ``NULL``，则返回 ``NULL``。

如果没有给出参数或参数不包含小数秒部分，则返回值为整数；如果给出参数包含小数秒部分，则返回值为 ``DECIMAL``。

## **函数语法**

```
> UNIX_TIMESTAMP([date])
```

## **参数释义**

|  参数   | 说明 |
|  ----  | ----  |
| date  | 可选参数。 date 参数是合法的日期表达式。<br> date 参数可以是 ``DATE``、``DATETIME`` 或 ``TIMESTAMP`` 字符串，也可以是 *YYMMDD*、*YYMMDDhhmmss*、*YYYYMMDD* 或 *YYYYMMDDhhmmss* 格式的数字。如果 date 参数包含时间部分，则它有选择地包含秒的小数部分。 <br>当 date 参数是 ``TIMESTAMP`` 时，``UNIX_TIMESTAMP()`` 直接返回内部时间戳值，而不进行隐含的 *string-to-Unix-timestamp* 转换。|

## **示例**

```sql
> SELECT UNIX_TIMESTAMP("2016-07-11");
+----------------------------+
| unix_timestamp(2016-07-11) |
+----------------------------+
|                 1468195200 |
+----------------------------+
```

## **限制**

目前date格式只支持 `yyyy-mm-dd` 和 `yyyymmdd` 的数据格式。  
