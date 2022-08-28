# **CHAR_LENGTH()**

## **函数说明**

`CHAR_LENGTH` 以字符为单位返回字符串 `str` 的长度,一个多字节字符算作一个字符。 一个汉字所对应的字符长度是1。

## **函数语法**

```
> CHAR_LENGTH(str)
```

## **参数释义**

|  参数   | 说明  |
|  ----  | ----  |
| str | 必要参数，想要计算长度的字符串 |

!!! note  "<font size=4>note</font>"
    <font size=3>`CHAR_LENGTH` 也可以写为 `lengthUTF8()`。</font>

## **示例**

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
