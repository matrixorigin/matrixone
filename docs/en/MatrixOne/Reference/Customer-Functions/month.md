# **MONTH()**

## **Description**

The MONTH() function returns the month part for a given date (a number from 1 to 12).


## **Syntax**

```
> MONTH(date)
```
## **Arguments**
|  Arguments   | Description  |
|  ----  | ----  |
| date  | Required.  The date/datetime to extract the month from |



## **Examples**


```sql
> drop table if exists t1;
> create table t1(a date);
> insert into t1 values('20211223');
> insert into t1 values('2021-12-24');

> select month(a) from t1;
+----------+
| month(a) |
+----------+
|       12 |
|       12 |
+----------+
```

## **Constraints**

The date type supports only `yyyy-mm-dd` and `yyyymmdd` for now. 

Currently, MatrixOne doesn't support select function() without from tables.
