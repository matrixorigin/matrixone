# **WEEKDAY()**

## **函数说明**

`WEEKDAY()`函数返回了输入日期在周内的序号(0 = 周一，1 = 周二， … 6 = 周日)。


## **函数语法**

```
> WEEKDAY(date)
```
## **参数释义**

|  参数   | 说明  |
|  ----  | ----  |
| date  | 必要参数  |



## **示例**


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

## **限制**

目前只支持`yyyy-mm-dd` 和 `yyyymmdd`的数据格式。  

MatrixOne目前只支持在查询表的时候使用函数，不支持单独使用函数。
