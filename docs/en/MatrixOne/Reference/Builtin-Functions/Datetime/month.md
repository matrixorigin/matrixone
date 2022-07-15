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
> create table t1(a date, b datetime);
> insert into t1 values('20211223','2021-10-22 09:23:23');
> insert into t1 values('2021-12-23','2021-10-22 00:23:23');

> select month(a) from t1;
+----------+----------+
| month(a) | month(b) |
+----------+----------+
|       12 |       10 |
|       12 |       10 |
+----------+----------+
```

## **Constraints**

The date type supports only `yyyy-mm-dd` and `yyyymmdd` for now. 
