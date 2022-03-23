# **AVG**

## **函数说明**

`AVG()`是聚合函数的一种，计算了参数列的算术平均值。


## **函数语法**

``` 
> AVG(expr)
```
## **参数释义**
|  参数   | 说明 |
|  ----  | ----  |
| expr  | 任何数值类型的列的列名 |

## **返回值**
以`Double`类型返回该列的算术平均值。
若输入参数为空，则返回`NaN`值。 

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

> select avg(col_1c) from tbl1;
+-------------+
| avg(col_1c) |
+-------------+
|      3.5000 |
+-------------+

> select sum(col_1d) as s1,avg(col_1d) as a3 from tbl1 group by col_1e order by s1 desc;
+------+---------+
| s1   | a3      |
+------+---------+
|   21 | 10.5000 |
|   18 |  9.0000 |
|   10 | 10.0000 |
|    8 |  8.0000 |
+------+---------+

> select avg(col_1d) as a1 from tbl1 where col_1d < 13 group by col_1e order by a1;
+---------+
| a1      |
+---------+
|  8.0000 |
|  9.0000 |
| 10.0000 |
| 10.5000 |
+---------+
```

## **限制**
MatrixOne目前只支持在查询表的时候使用函数，不支持单独使用函数。