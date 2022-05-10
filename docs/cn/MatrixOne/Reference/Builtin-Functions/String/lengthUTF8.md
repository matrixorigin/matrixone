# **LENGTHUTF8()**

## **函数说明**

`lengthUTF8()`以字符为单位返回字符串str的长度,一个多字节字符算作一个字符。 一个汉字所对应的字符长度是1。


## **函数语法**

```
> LENGTHUTF8(str)
```
## **参数释义**
|  参数   | 说明  |
|  ----  | ----  |
| str | 必要参数，想要计算长度的字符串 |


## **示例**


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

## **限制**
MatrixOne目前只支持在查询表的时候使用函数，不支持单独使用函数。
