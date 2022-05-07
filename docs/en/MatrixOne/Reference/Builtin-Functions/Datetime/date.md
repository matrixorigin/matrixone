# **DATE()**

## **Description**

Extracts the date part of the date or datetime expression expr.


## **Syntax**

```
> DATE(expr)
```
## **Arguments**
|  Arguments   | Description  |
|  ----  | ----  |
| expr  | Required.  The date/datetime to extract the date from. |



## **Examples**


```sql
> drop table if exists t1;
> create table t1(a date, b datetime);
> insert into t1 values('2022-01-01','2022-01-01 01:01:01');
> insert into t1 values('2022-01-01','2022-01-01 01:01:01');
> insert into t1 values(20220101,'2022-01-01 01:01:01');
> insert into t1 values('2022-01-02','2022-01-02 23:01:01');
> insert into t1 values('2021-12-31','2021-12-30 23:59:59');
> insert into t1 values('2022-06-30','2021-12-30 23:59:59');
> select date(a),date(b) from t1;
+------------+------------+
| date(a)    | date(b)    |
+------------+------------+
| 2022-01-01 | 2022-01-01 |
| 2022-01-01 | 2022-01-01 |
| 2022-01-01 | 2022-01-01 |
| 2022-01-02 | 2022-01-02 |
| 2021-12-31 | 2021-12-30 |
| 2022-06-30 | 2021-12-30 |
+------------+------------+
> select date(a),date(date(a)) as dda from t1;
+------------+------------+
| date(a)    | dda        |
+------------+------------+
| 2022-01-01 | 2022-01-01 |
| 2022-01-01 | 2022-01-01 |
| 2022-01-01 | 2022-01-01 |
| 2022-01-02 | 2022-01-02 |
| 2021-12-31 | 2021-12-31 |
| 2022-06-30 | 2022-06-30 |
+------------+------------+
```

## **Constraints**

The date type supports only `yyyy-mm-dd` and `yyyymmdd` for now. 

Currently, MatrixOne doesn't support select function() without from tables.
