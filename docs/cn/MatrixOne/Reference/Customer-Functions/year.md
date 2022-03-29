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

## **限制**

目前只支持`yyyy-mm-dd` 和 `yyyymmddd`的数据格式。

## **限制**
MatrixOne目前只支持在查询表的时候使用函数，不支持单独使用函数。