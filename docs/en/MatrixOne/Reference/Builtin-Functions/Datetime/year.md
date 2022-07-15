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
> create table t1(a date, b datetime);
> insert into t1 values('20211223','2021-10-22 09:23:23');
> insert into t1 values('2021-12-23','2021-10-22 00:23:23');

> select year(a), toyear(b)from t1;
+---------+-----------+
| year(a) | toyear(b) |
+---------+-----------+
|    2021 |      2021 |
|    2021 |      2021 |
+---------+-----------+
```

## **Constraints**

The date type supports only `yyyy-mm-dd` and `yyyymmdd` for now.
