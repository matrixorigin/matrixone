# **YEAR()**

## **Description**

The YEAR() or TOYEAR() function returns the year part for a given date (a number from 1000 to 9999).


## **Syntax**

```
> YEAR(date)
> TOYEAR(date)
```
## **Arguments**
|  Arguments   | Description  |
|  ----  | ----  |
| date  | Required.  The date/datetime to extract the year from |



## **Examples**


```sql
> drop table if exists t1;
> create table t1(a date);
> insert into t1 values('20211223');
> insert into t1 values('2021-12-24');

> select toyear(a) from t1;
+---------+
| year(a) |
+---------+
|    2021 |
|    2021 |
+---------+

> select year(a) from t1;
+---------+
| year(a) |
+---------+
|    2021 |
|    2021 |
+---------+
```

## **Constraints**

The date type supports only `yyyy-mm-dd` and `yyyymmddd` for now. 

Currently, MatrixOne doesn't support select function() without from tables.
