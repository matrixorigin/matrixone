# **CONCAT_WS()**

## **函数说明**

``CONCAT_WS()``代表Concatenate With Separator，是 ``CONCAT()`` 的一种特殊形式。第一个参数是其它参数的分隔符。分隔符的位置放在要连接的两个字符串之间。分隔符可以是字符串，也可以是其他参数。如果分隔符为 ``NULL``，则结果为 ``NULL``。函数会忽略任何分隔符参数后的 ``NULL`` 值。

## **函数语法**

- 函数语法1

```
> CONCAT_WS(separator,str1,str2,...)
```

- 函数语法2

```
> CONCAT_WS(separator,str1,NULL,str1,...);
```

## **参数释义**

|  参数   | 说明  |
|  ----  | ----  |
| Str | 必要参数. 需要翻转的字符串. CHAR与VARCHAR类型均可. |

## **示例**

```SQL
SELECT CONCAT_WS(',','First name','Second name','Last Name');
+--------------------------------------------------+
| concat_ws(,, First name, Second name, Last Name) |
+--------------------------------------------------+
| First name,Second name,Last Name                 |
+--------------------------------------------------+
1 row in set (0.01 sec)
> SELECT CONCAT_WS(',','First name',NULL,'Last Name');
+-------------------------------------------+
| concat_ws(,, First name, null, Last Name) |
+-------------------------------------------+
| First name,Last Name                      |
+-------------------------------------------+
1 row in set (0.01 sec)
```
