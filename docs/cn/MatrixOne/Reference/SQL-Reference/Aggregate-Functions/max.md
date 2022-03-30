# **MAX**

## **函数说明**

`MAX()`是聚合函数的一种，计算了一组值的最大值。


## **函数语法**

```
> MAX(expr)
```

## **参数释义**
|  参数  | 说明 |
|  ----  | ----  |
| expr  | 任何数值类型与字符串列的列名|

## **返回值**
返回`expr`列中的最大值，同时也可以在`MAX()`中使用字符串，如此会返回最大的字符串值。

## **示例**


```sql
> drop table if exists tbl1,tbl2;
> create table tbl1 (col_1a tinyint, col_1b smallint, col_1c int, col_1d bigint, col_1e char(10) not null);
> insert into tbl1 values (0,1,1,7,"a");
> insert into tbl1 values (0,1,2,8,"b");
> insert into tbl1 values (0,1,3,9,"c");
> insert into tbl1 values (0,1,4,10,"D");
> insert into tbl1 values (0,1,5,11,"a");
> insert into tbl1 values (0,1,6,12,"c");

> select max(col_1d) from tbl1;
+-------------+
| max(col_1d) |
+-------------+
|          12 |
+-------------+

> select max(col_1c) as m1 from tbl1 where col_1d<12 group by col_1e;
+------+
| m1   |
+------+
|    5 |
|    2 |
|    3 |
|    4 |
+------+
```

***
## **限制**
MatrixOne目前只支持在查询表的时候使用函数，不支持单独使用函数。
