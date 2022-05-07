# **STARTSWITH()**

## **函数说明**

字符串如果以指定前缀开始返回1， 否则则返回0。该函数是对大小写敏感的。

## **函数语法**

```
> STARTSWITH(str,prefix)
```
## **参数释义**
|  参数   | 说明  |
|  ----  | ----  |
| str | 必要参数.  CHAR和VARCHAR类型都支持. |
| prefix | 必要参数.  CHAR和VARCHAR类型都支持. |

## **返回值**
* 1, 如果字符串是以指定前缀开始的。
* 0, 如果字符串不以指定前缀开始的。

## **示例**

```sql
> drop table if exists t1;
> create table t1(a int,b varchar(100),c char(20));
> insert into t1 values
(1,'Ananya Majumdar', 'IX'),
(2,'Anushka Samanta', 'X'),
(3,'Aniket Sharma', 'XI'),
(4,'Anik Das', 'X'),
(5,'Riya Jain', 'IX'),
(6,'Tapan Samanta', 'X');
> select a,startswith(b,'An') from t1;
+------+-------------------+
| a    | startswith(b, An) |
+------+-------------------+
|    1 |                 1 |
|    2 |                 1 |
|    3 |                 1 |
|    4 |                 1 |
|    5 |                 0 |
|    6 |                 0 |
+------+-------------------+
> select a,b,c from t1 where startswith(b,'An')=1 and startswith(c,'I')=1;
+------+-----------------+------+
| a    | b               | c    |
+------+-----------------+------+
|    1 | Ananya Majumdar | IX   |
+------+-----------------+------+
```

## **限制**
MatrixOne目前只支持在查询表的时候使用函数，不支持单独使用函数。