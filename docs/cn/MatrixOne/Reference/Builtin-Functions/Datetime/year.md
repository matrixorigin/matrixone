# **YEAR()**

## **函数说明**

`YEAR()`和`TOYEAR()`函数返回了给定日期的年份（从1000到9999）。

## **函数语法**

```
> YEAR(date)
> TOYEAR(date)
```

## **参数释义**

|  参数  | 说明  |
|  ----  | ----  |
| date  | 必要参数，需要提取年份的日期 |

## **示例**

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

## **限制**

目前只支持`yyyy-mm-dd` 和 `yyyymmddd`的数据格式。
