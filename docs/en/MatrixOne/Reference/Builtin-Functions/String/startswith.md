# **STARTSWITH()**

## **Description**

Returns 1 whether string starts with the specified prefix, otherwise it returns 0.This function is case sensitive.

## **Syntax**

```
> STARTSWITH(str,prefix)
```

## **Arguments**

|  Arguments   | Description  |
|  ----  | ----  |
| str | Required.  CHAR and VARCHAR both are supported.|
| prefix | Required.  CHAR and VARCHAR both are supported.|

## **Returned Values**

* 1, if the string starts with the specified prefix.
* 0, if the string does not start with the specified prefix.

## **Examples**

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
