# **HEX()**

## **Description**

For a string argument str, `HEX()` returns a hexadecimal string representation of str where each byte of each character in str is converted to two hexadecimal digits. (Multibyte characters therefore become more than two digits.)

For a numeric argument N, `HEX()` returns a hexadecimal string representation of the value of N treated as an integer number. This is equivalent to `CONV(N,10,16)`. The inverse of this operation is performed by `CONV(HEX(N),16,10)`.

For a `NULL` argument, this function returns `NULL`.

## **Syntax**

```
> HEX(str), HEX(N)
```

## **Arguments**

|  Arguments   | Description  |
|  ----  | ----  |
| N | Required. A number which is to be converted to hexadecimal. |
| str | Required. A string whose each character is to be converted to two hexadecimal digits. |

## **Examples**

```SQL
> SELECT HEX('abc');
+----------+
| hex(abc) |
+----------+
| 616263   |
+----------+
1 row in set (0.00 sec)

> SELECT HEX(255);
+----------+
| hex(255) |
+----------+
| FF       |
+----------+
1 row in set (0.00 sec)
```
