# **TO_DATE()**

## **Description**

``STR_TO_DATE()`` returns a DATETIME value if the format string contains both date and time parts, or a DATE or TIME value if the string contains only date or time parts.

## **Syntax**

```
> TO_DATE(str,format)
```

## **Arguments**

|  Arguments   | Description  |
|  ----  | ----  |
| str  | Required.  <br>If the date, time, or datetime value extracted from str is illegal, ``STR_TO_DATE()`` returns ``NULL`` and produces a warning. <br>If ``str`` is ``NULL``, the function returns ``NULL``.  |
| format  | Required.  A format string indicating the format of the return value.<br> If the format is omitted, this function returns a DATETIME value. <br>If the format is ``NULL``, this function returns ``NULL``.<br>If the format is supplied, the value returned is a VARCHAR. |

!!! note  "<font size=4>note</font>"
    <font size=3>The format string can contain literal characters and format specifiers beginning with %. Literal characters in ``format`` must match literally in str. Format specifiers in ``format`` must match a date or time part in ``str``.</font>  

## **Examples**

```sql
> SELECT TO_DATE('2022-01-06 10:20:30','%Y-%m-%d %H:%i:%s') as result;
+---------------------+
| result              |
+---------------------+
| 2022-01-06 10:20:30 |
+---------------------+                   
```

## **Constraints**

The date type supports only `yyyy-mm-dd` and `yyyymmdd` for now.
