# **OCT(N)**

## **Description**

This function ``OCT(N)`` returns a string representation of the octal value of *N*, where *N* is a longlong (BIGINT) number. Returns ``NULL`` if *N* is *NULL*.

## **Syntax**

```
> OCT(N)
```

## **Arguments**

|  Arguments   | Description  |
|  ----  | ----  |
| N | Required. UINT Type |

## **Examples**

```SQL
SELECT OCT(12);
+---------+
| oct(12) |
+---------+
| 14.0000 |
+---------+
1 row in set (0.00 sec)
```
