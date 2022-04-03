# **WEEKDAY()**

## **Description**

This function returns the weekday index for date (0 = Monday, 1 = Tuesday, … 6 = Sunday).


## **Syntax**

```
> WEEKDAY(date)
```
## **Arguments**
|  Arguments   | Description  |
|  ----  | ----  |
| date  | Required.  |



## **Examples**


```sql
> drop table if exists t1;
>  create table t1(a date,b datetime);
> insert into t1 values('20220202','2021-12-24 09:23:23');
> insert into t1 values('2022-02-02','2021-12-24');

> select weekday(a),weekday(b) from t1;
+------------+------------+
| weekday(a) | weekday(b) |
+------------+------------+
|          2 |          4 |
|          2 |          4 |
+------------+------------+
```

## **Constraints**

The date type supports only `yyyy-mm-dd` and `yyyymmdd` for now. 

Currently, MatrixOne doesn't support select function() without from tables.
