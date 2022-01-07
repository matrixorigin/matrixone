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


The datatype can be one of the following:

|  Value   | Description  |
|  ----  | ----  |
| DATE  | Converts value to DATE. Format: "YYYY-MM-DD" |
| DATETIME  | Converts value to DATETIME. Format: "YYYY-MM-DD HH:MM:SS" |
| DECIMAL  | Converts value to DECIMAL. Use the optional M and D parameters to specify the maximum number of digits (M) and the number of digits following the decimal point (D). |
| TIME  | Converts value to TIME. Format: "HH:MM:SS" |
| CHAR  | Converts value to CHAR (a fixed length string) |
| NCHAR  | Converts value to NCHAR (like CHAR, but produces a string with the national character set) |
| SIGNED  | Converts value to SIGNED (a signed 64-bit integer) |
| UNSIGNED  | Converts value to UNSIGNED (an unsigned 64-bit integer) |
| BINARY  | Converts value to BINARY (a binary string) |


## **Examples**
```

#Convert a value to a DATE datatype:

> SELECT CAST("2017-08-29" AS DATE);

#Convert a value to a CHAR datatype:

> SELECT CAST(150 AS CHAR);

#Convert a value to a SIGNED datatype:

>SELECT CAST(5-10 AS SIGNED);

```

## **Constraints**

