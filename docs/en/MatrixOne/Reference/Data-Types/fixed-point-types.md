# Fixed-Point Types (Exact Value) - DECIMAL

The DECIMAL type store exact numeric data values. These types are used when it is important to preserve exact precision, for example with monetary data, or with scientific calculations. 

In a DECIMAL column declaration, the precision and scale can be (and usually is) specified. For example:

```sql
salary DECIMAL(5,2)
```

In this example, 5 is the precision and 2 is the scale. The precision represents the number of significant digits that are stored for values, and the scale represents the number of digits that can be stored following the decimal point.

Standard SQL requires that DECIMAL(5,2) be able to store any value with five digits and two decimals, so values that can be stored in the salary column range from -999.99 to 999.99.

In MatrixOne, the syntax DECIMAL(M) is equivalent to DECIMAL(M,0). Similarly, the syntax DECIMAL is equivalent to DECIMAL(M,0), where the implementation is permitted to decide the value of M. MatrixOne supports both of these variant forms of DECIMAL syntax. The default value of M is 10.

If the scale is 0, DECIMAL values contain no decimal point or fractional part.

In MatrixOne, the maximum number of digits for DECIMAL is 38, but the actual range for a given DECIMAL column can be constrained by the precision or scale for a given column. When such a column is assigned a value with more digits following the decimal point than are permitted by the specified scale, the value is converted to that scale.

## DECIMAL Data Type Characteristics

This section discusses the characteristics of the DECIMAL data type (and its synonyms), with particular regard to the following topics:

* Maximum number of digits

* Storage format

The declaration syntax for a DECIMAL column is DECIMAL(M,D). The ranges of values for the arguments are as follows:

M is the maximum number of digits (the precision). It has a range of 1 to 38.

D is the number of digits to the right of the decimal point (the scale). It has a range of 1 to 38 and must be no larger than M.

If D is omitted, the default is 0. If M is omitted, the default is 10.

The maximum value of 38 for M means that calculations on DECIMAL values are accurate up to 38 digits. 

Values for DECIMAL columns are stored using a binary format that packs decimal digits into 8 bytes or 16 bytes. The storage required for remaining digits is given by the following table.

|  Digits   | Number of Bytes  |
|  ----  | ----  |
|  0-18  | 8 bytes  |
|  19-38  | 16 bytes  |

For a full explanation of the internal implementation of DECIMAL values, see the [Feature Design](https://github.com/matrixorigin/matrixone/issues/1867).
