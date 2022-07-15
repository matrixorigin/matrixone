# **LENGTH()**

## **函数说明**

`length()`函数返回了字符串的长度。

## **函数语法**

```
> LENGTH(str)
```

## **参数释义**

|  参数   | 说明  |
|  ----  | ----  |
| str | 必要参数，想要计算长度的字符串 |

## **示例**

```sql
> select a,length(a) from t1;
a	length(a)
a       1
ab      2
abc     3
```
