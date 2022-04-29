# **ATAN()**

## **函数说明**

ATAN()函数返回给定数值的反正切（用弧度表示）。


## **函数语法**

```
> ATAN(number)
```
## **参数释义**
|  参数   | 说明  |
|  ----  | ----  |
| number | 必要参数，想要进行舍入的数值，可取任意数值数据类型 |


## **示例**

```sql
> drop table if exists t1;
> create table t1(a int,b float);
> insert into t1 values(1,3.14159);
> insert into t1 values(0,1);
> select atan(a),atan(tan(b)) from t1;
+---------+--------------+
| atan(a) | atan(tan(b)) |
+---------+--------------+
|  0.7854 |      -0.0000 |
|  0.0000 |       1.0000 |
+---------+--------------+

```

## **限制**
MatrixOne目前只支持在查询表的时候使用函数，不支持单独使用函数。
