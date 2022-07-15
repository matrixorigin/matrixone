# **EMPTY()**

## **Description**

Checks whether the input string is empty.
A string is considered non-empty if it contains at least one byte, even if this is a space or a null byte.

## **Syntax**

```
> EMPTY(str)
```

## **Arguments**

|  Arguments   | Description  |
|  ----  | ----  |
| str | Required. both CHAR and VARCHAR are supported. |

## **Returned Values**

Returns 1 for an empty string or 0 for a non-empty string.

## **Examples**

```SQL
> drop table if exists t1;
> create table t1(a varchar(255),b varchar(255));
> insert into t1 values('', 'abcd');
> insert into t1 values('1111', '');
> select empty(a),empty(b) from t1;
+----------+----------+
| empty(a) | empty(b) |
+----------+----------+
|        1 |        0 |
|        0 |        1 |
+----------+----------+
```
