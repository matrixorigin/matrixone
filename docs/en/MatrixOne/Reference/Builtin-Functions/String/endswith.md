# **ENDSWITH()**

## **Description**

Returns whether to end with the specified suffix. Returns 1 if the string ends with the specified suffix, otherwise it returns 0. This function is case sensitive.

## **Syntax**

```
> ENDSWITH(str,suffix)
```

## **Arguments**

|  Arguments   | Description  |
|  ----  | ----  |
| str | Required.  CHAR and VARCHAR both are supported.|
| suffix | Required.  CHAR and VARCHAR both are supported.|

## **Returned Values**

* 1, if the string ends with the specified suffix.
* 0, if the string does not end with the specified suffix.

## **Examples**

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
