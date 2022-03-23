# **CAST**

## **Description**
The CAST() function converts a value (of any type) into the specified datatype.

## **Syntax**
```
> CAST(value AS datatype)

```

## **Parameter Values**
|  Parameter   | Description  |
|  ----  | ----  |
| value  | Required. The value to convert |
| datatype  | Required. The datatype to convert to |



Currently, `cast` can support following conversion:
 * Conversion between numeric types, mainly including SIGNED, UNSIGNED, FLOAT, and DOUBLE type.
 * Numeric types to character CHAR type.
 * Numeric character types to numerical types(negative into SIGNED).

## **Examples**


```sql
> drop table if exists t1;
> CREATE TABLE t1 (a int,b float,c char(1),d varchar(15));
> INSERT INTO t1 VALUES (1,1.5,'1','-2');
> SELECT CAST(a AS FLOAT) a_cast,CAST(b AS UNSIGNED) b_cast,CAST(c AS SIGNED) c_cast, CAST(d AS SIGNED) d_cast from t1;
+--------+--------+--------+--------+
| a_cast | b_cast | c_cast | d_cast |
+--------+--------+--------+--------+
| 1.0000 |      1 |      1 |     -2 |
+--------+--------+--------+--------+

> SELECT CAST(a AS CHAR) a_cast, CAST(b AS CHAR) b_cast,CAST(c AS DOUBLE) c_cast, CAST(d AS FLOAT) d_cast from t1;
+--------+--------+--------+---------+
| a_cast | b_cast | c_cast | d_cast  |
+--------+--------+--------+---------+
| 1      | 1.5    | 1.0000 | -2.0000 |
+--------+--------+--------+---------+
```


## **Constraints**
Currently, MatrixOne doesn't support select function() without from tables.

* Non-numeric character types cannot be converted to numeric types.
* Numeric and character types with formats of Data cannot be converted to Date.
* Date and Datetime types cannot be converted to character types.
* Date and Datetime cannot be converted to each other.


