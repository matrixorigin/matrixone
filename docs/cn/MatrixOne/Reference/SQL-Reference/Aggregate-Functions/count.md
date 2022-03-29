# **COUNT**

## **函数说明n**

`COUNT()`是聚合函数的一种，计算了查询结果的记录数（`NULL`值不参与统计）。


## **函数语法**

```
> COUNT(expr)
```
***

## **参数释义**
| 参数   | 说明 |
|  ----  | ----  |
| expr  | 任何查询结果，既可以是列名，也可以是一个函数或者数学运算的结果。也可以使用`*`，直接统计行数。|

## **返回值**
返回查询结果中`expr`列的`NOT NULL`的值的个数，返回数据类型为`BIGINT`。
如果没有匹配的行，将返回0。



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

> select count(col_1b) from tbl1;
+---------------+
| count(col_1b) |
+---------------+
|             6 |
+---------------+

> select count(*) from tbl1 where col_1d<10;
+----------+
| count(*) |
+----------+
|        3 |
+----------+
```

## **限制**
MatrixOne目前只支持在查询表的时候使用函数，不支持单独使用函数。

