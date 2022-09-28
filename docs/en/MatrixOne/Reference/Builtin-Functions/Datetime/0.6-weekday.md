# **WEEKDAY()**

## **Description**

Returns the weekday index for date (0 = Monday, 1 = Tuesday, … 6 = Sunday). Returns `NULL` if date is `NULL`.

## **Syntax**

```
> WEEKDAY(date)
```

### **Arguments**

|  Arguments   | Description  |
|  ----  | ----  |
| date  | Required. The date to be formatted. |

## **Examples**

- Example 1:

```sql
> SELECT WEEKDAY('2008-02-03 22:23:00');
+------------------------------+
| weekday(2008-02-03 22:23:00) |
+------------------------------+
|                            6 |
+------------------------------+
1 row in set (0.03 sec)
```

- Example 2:

```sql
> drop table if exists t1;
> create table t1 (id int,d date, dt datetime,c char(10),vc varchar(20));
> insert into t1 values (1,"2021-01-13", "2021-01-13 13:00:00", "2021-12-15", "2021-12-16");
> insert into t1 values (1,"2021-01-31", "2021-01-31 13:00:00", "2021-12-15", "2021-12-16");
> insert into t1 values (2,"2022-02-15", "2022-02-15 18:54:29", "2021-02-15", "2021-02-15");
> insert into t1 values (2,"2022-02-28", "2022-02-28 18:54:29", "2021-02-15", "2021-02-15");
> insert into t1 values (3,"2000-02-29", "2000-02-29 18:54:29", "2021-02-15", "2021-02-15");
> insert into t1 values (4,"2023-03-17", "2021-02-17 23:54:59", "2021-03-17", "2021-03-17");
> insert into t1 values (5,"1985-04-18", "1985-04-18 00:00:01", "1985-04-18", "1985-04-18");
> insert into t1 values (6,"1987-05-20", "1987-05-20 22:59:59", "1987-05-20", "1987-05-20");
> insert into t1 values (7,"1989-06-22", "1989-06-22 15:00:30", "1989-06-22", "1989-06-22");
> insert into t1 values (8,"1993-07-25", "1987-07-25 03:04:59", "1993-07-25", "1993-07-25");
> insert into t1 values (9,"1995-08-27", "1987-08-27 04:32:33", "1995-08-27", "1995-08-27");
> insert into t1 values (10,"1999-09-30", "1999-09-30 10:11:12", "1999-09-30", "1999-09-30");
> insert into t1 values (11,"2005-10-30", "2005-10-30 18:18:59", "2005-10-30", "2005-10-30");
> insert into t1 values (12,"2008-11-30", "2008-11-30 22:59:59", "2008-11-30", "2008-11-30");
> insert into t1 values (13,"2013-12-01", "2013-12-01 22:59:59", "2013-12-01", "2013-12-01");
> insert into t1 values (14,null, null, null, null);
> select weekday(d),weekday(dt) from t1;
+------------+-------------+
| weekday(d) | weekday(dt) |
+------------+-------------+
|          2 |           2 |
|          6 |           6 |
|          1 |           1 |
|          0 |           0 |
|          1 |           1 |
|          4 |           2 |
|          3 |           3 |
|          2 |           2 |
|          3 |           3 |
|          6 |           5 |
|          6 |           3 |
|          3 |           3 |
|          6 |           6 |
|          6 |           6 |
|          6 |           6 |
|       NULL |        NULL |
+------------+-------------+
16 rows in set (0.01 sec)

> select weekday(c),weekday(vc) from t1;
+------------+-------------+
| weekday(c) | weekday(vc) |
+------------+-------------+
|          2 |           3 |
|          2 |           3 |
|          0 |           0 |
|          0 |           0 |
|          0 |           0 |
|          2 |           2 |
|          3 |           3 |
|          2 |           2 |
|          3 |           3 |
|          6 |           6 |
|          6 |           6 |
|          3 |           3 |
|          6 |           6 |
|          6 |           6 |
|          6 |           6 |
|       NULL |        NULL |
+------------+-------------+
16 rows in set (0.02 sec)
```
