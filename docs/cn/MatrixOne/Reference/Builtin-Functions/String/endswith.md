# **ENDSWITH()**

## **函数说明**

检查是否以指定后缀结尾。字符串如果以指定后缀结尾返回1， 否则则返回0。该函数是对大小写敏感的。


## **函数语法**

```
> ENDSWITH(str,suffix)
```
## **参数释义**
|  参数   | 说明  |
|  ----  | ----  |
| str | 必要参数.  CHAR和VARCHAR类型都支持.|
| suffix | 必要参数.  CHAR和VARCHAR类型都支持.|

## **返回值**
* 1, 如果字符串是以指定后缀结尾的。
* 0, 如果字符串不以指定后缀结尾的。

## **示例**

```sql
> drop table if exists t1;
> create table t1(a int,b varchar(100),c char(20));
> insert into t1 values
(1,'Ananya Majumdar', 'XI'),
(2,'Anushka Samanta', 'X'),
(3,'Aniket Sharma', 'XI'),
(4,'Anik Das', 'X'),
(5,'Riya Jain', 'IX'),
(6,'Tapan Samanta', 'XI');
> select a,endsWith(b,'a') from t1;
+------+----------------+
| a    | endswith(b, a) |
+------+----------------+
|    1 |              0 |
|    2 |              1 |
|    3 |              1 |
|    4 |              0 |
|    5 |              0 |
|    6 |              1 |
+------+----------------+
> select a,b,c from t1 where endswith(b,'a')=1 and endswith(c,'I')=1;
+------+---------------+------+
| a    | b             | c    |
+------+---------------+------+
|    3 | Aniket Sharma | XI   |
|    6 | Tapan Samanta | XI   |
+------+---------------+------+
```

## **限制**
MatrixOne目前只支持在查询表的时候使用函数，不支持单独使用函数。