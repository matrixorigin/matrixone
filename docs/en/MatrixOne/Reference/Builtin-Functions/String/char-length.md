# **CHAR_LENGTH()**

## **Description**

The `CHAR_LENGTH` function returns the length of the string str, measured in code points. A multibyte character counts as a single code point. This means that, for a string containing two 3-byte characters, `CHAR_LENGTH` returns 6, whereas `CHAR_LENGTH` returns 2.  

## **Syntax**

```
> CHAR_LENGTH(str)
```

!!! note  "<font size=4>note</font>"
    <font size=3>The alias for `CHAR_LENGTH` can also be `lengthUTF8()`</font>

## **Arguments**

|  Arguments   | Description  |
|  ----  | ----  |
| str | Required. String you want to calculate. |

## **Examples**

```sql
> drop table if exists t1;
> create table t1(a varchar(255),b varchar(255));
> insert into t1 values('nihao','你好');
> select char_length(a), char_length(b) from t1;
+---------------+---------------+
| lengthutf8(a) | lengthutf8(b) |
+---------------+---------------+
|             5 |             2 |
+---------------+---------------+

```
