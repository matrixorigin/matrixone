# **CONCAT_WS()**

## **Description**

This function ``CONCAT_WS()`` stands for Concatenate With Separator and is a special form of ``CONCAT()``. The first argument is the separator for the rest of the arguments. The separator is added between the strings to be concatenated. The separator can be a string, as can the rest of the arguments. If the separator is ``NULL``, the result is ``NULL``.

## **Syntax**

- Syntax 1

```
> CONCAT_WS(separator,str1,str2,...)
```

- Syntax 2

```
> CONCAT_WS(separator,str1,NULL,str1,...);
```

## **Arguments**

|  Arguments   | Description  |
|  ----  | ----  |
| str | Required. both CHAR and VARCHAR are supported. |

## **Examples**

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
