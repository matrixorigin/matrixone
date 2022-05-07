# **COT()**

## **函数说明**

COT()函数返回输入参数（用弧度表示）的余切值。


## **函数语法**

```
> COT(number)
```
## **参数释义**
|  参数   | 说明  |
|  ----  | ----  |
| number | 必要参数，可取任意数值数据类型 |



## **示例**

```sql
> drop table if exists t1;
> create table t1(a int,b float);
> insert into t1 values(1,3.14159);
> insert into t1 values(-1,0);
> select cot(a),cot(b) from t1;
+---------+--------+
| cot(a)  | cot(b) |
+---------+--------+
| -0.5574 | 1.0000 |
|  2.5574 | 1.0000 |
+---------+--------+
```

## **限制**
MatrixOne目前只支持在查询表的时候使用函数，不支持单独使用函数。