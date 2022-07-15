# **LENGTHUTF8()**

## **Description**

The lengthUTF8() function returns the length of the string str, measured in code points. A multibyte character counts as a single code point. This means that, for a string containing two 3-byte characters, LENGTH() returns 6, whereas LENGTHUTF8() returns 2.  

## **Syntax**

```
> LENGTHUTF8(str)
```

## **Arguments**

|  Arguments   | Description  |
|  ----  | ----  |
| str | Required. String you want to calculate. |

## **Examples**

```sql
> drop table if exists t1;
> create table t1(a varchar(255),b varchar(255));
> insert into t1 values('nihao','你好');
> select lengthUTF8(a), lengthUTF8(b) from t1;
+---------------+---------------+
| lengthutf8(a) | lengthutf8(b) |
+---------------+---------------+
|             5 |             2 |
+---------------+---------------+

```
