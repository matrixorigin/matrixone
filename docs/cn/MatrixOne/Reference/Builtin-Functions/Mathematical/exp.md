# **EXP()**

## **函数说明**

EXP(number)函数返回以自然常数e为底的number的指数。

## **函数语法**

```
> EXP(number)
```
## **参数释义**
|  参数   | 说明  |
|  ----  | ----  |
| number | 必要参数，可取任意数值数据类型 |




## **示例**

```sql
> drop table if exists t1;
> create table t1(a int ,b float);
> insert into t1 values(-4, 2.45);
> insert into t1 values(6, -3.62);
> select exp(a), exp(b) from t1;
+----------+---------+
| exp(a)   | exp(b)  |
+----------+---------+
|   0.0183 | 11.5883 |
| 403.4288 |  0.0268 |
+----------+---------+

```

## **限制**
MatrixOne目前只支持在查询表的时候使用函数，不支持单独使用函数。
