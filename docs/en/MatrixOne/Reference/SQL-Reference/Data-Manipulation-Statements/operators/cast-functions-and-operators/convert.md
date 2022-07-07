# **CONVERT**

## **Description**

The `CONVERT()` function converts a value into the specified datatype or character set.

## **Syntax**

```
> CONVERT(value, type)

```

Or:

```
> CONVERT(value USING charset)
```

## **Parameter Values**

|  Parameter   | Description  |
|  ----  | ----  |
| value  | Required. The value to convert. |
| datatype  | Required. The datatype to convert to. |
| charset |	Required. The character set to convert to. |

Currently, `convert` can support following conversion:

 * Conversion between numeric types, mainly including SIGNED, UNSIGNED, FLOAT, and DOUBLE type.
 * Numeric types to character CHAR type.
 * Numeric character types to numerical types(negative into SIGNED).

## **Examples**

```sql
> select convert(150,char);
+-------------------+
| cast(150 as char) |
+-------------------+
| 150               |
+-------------------+
1 row in set (0.01 sec)
```

```sql
> CREATE TABLE t1(a tinyint);
> INSERT INTO t1 VALUES (127);
> SELECT 1 FROM
  -> (SELECT CONVERT(t2.a USING UTF8) FROM t1, t1 t2 LIMIT 1) AS s LIMIT 1;
+------+
| 1    |
+------+
|    1 |
+------+
1 row in set (0.00 sec)
```

## **Constraints**

* Non-numeric character types cannot be converted to numeric types.
* Numeric and character types with formats of Data cannot be converted to Date.
* Date and Datetime types cannot be converted to character types.
* Date and Datetime cannot be converted to each other.
