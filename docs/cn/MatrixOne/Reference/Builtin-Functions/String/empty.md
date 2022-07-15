# **EMPTY()**

## **函数说明**

判断输入的字符串是否为空。如果包含至少一个字节则不为空，即使是一个空格或者NULL。

## **函数语法**

```
> EMPTY(str)
```

## **参数释义**

|  参数   | 说明  |
|  ----  | ----  |
| str | 必要参数，CHAR与VARCHAR类型均可。 |

## **返回值**

空字符串返回1，非空字符串返回0.

## **示例**

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
