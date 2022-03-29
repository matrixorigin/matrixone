# **SUM**

## **函数说明**


`SUM()`聚合函数计算了一组值的和（NULL值被忽略）。

## **函数语法**

```
> SUM(expr)
```

## **参数释义**
|  参数  | 说明 |
|  ----  | ----  |
| expr  | 任何数值类型与字符串列的列名|

## **返回值**
返回`expr`列的数值的和，若输入参数为`Double`类型，则返回值为`Double`，否则为整数类型。  
如果没有匹配的行，则返回`NULL`值。

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

> select sum(col_1c) from tbl1;
+-------------+
| sum(col_1c) |
+-------------+
|          21 |
+-------------+

> select sum(col_1d) as c1 from tbl1 where col_1d < 13 group by col_1e order by c1;
+------+
| c1   |
+------+
|    8 |
|   10 |
|   18 |
|   21 |
+------+
```
***


## **限制**
MatrixOne目前只支持在查询表的时候使用函数，不支持单独使用函数。