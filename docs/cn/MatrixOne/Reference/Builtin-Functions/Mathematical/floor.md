# **FLOOR()**

## **函数说明**
`FLOOR()`函数返回不大于某个数字的相应数位的数。



## **函数语法**

```
> FLOOR(number, decimals)
> FLOOR(number)
```
## **参数释义**
|  参数  | 说明  |
|  ----  | ----  |
| number | 必要参数，任何当前支持的数值数据 |
| decimals| 可选参数，代表小数点后的位数。默认值为0，代表四舍五入为整数，当为负数时四舍五入到小数点前的数位。|



## **示例**

```sql
> drop table if exists t1;
> create table t1(a int ,b float);
> insert into t1 values(1,0.5);
> insert into t1 values(2,0.499);
> insert into t1 values(3,0.501);
> insert into t1 values(4,20.5);
> insert into t1 values(5,20.499);
> insert into t1 values(6,13.500);
> insert into t1 values(7,-0.500);
> insert into t1 values(8,-0.499);
> insert into t1 values(9,-0.501);
> insert into t1 values(10,-20.499);
> insert into t1 values(11,-20.500);
> insert into t1 values(12,-13.500);
> select a,floor(b) from t1;
+------+----------+
| a    | floor(b) |
+------+----------+
|    1 |   0.0000 |
|    2 |   0.0000 |
|    3 |   0.0000 |
|    4 |  20.0000 |
|    5 |  20.0000 |
|    6 |  13.0000 |
|    7 |  -1.0000 |
|    8 |  -1.0000 |
|    9 |  -1.0000 |
|   10 | -21.0000 |
|   11 | -21.0000 |
|   12 | -14.0000 |
+------+----------+
> select sum(floor(b)) from t1;
+---------------+
| sum(floor(b)) |
+---------------+
|       -6.0000 |
+---------------+
> select a,sum(floor(b)) from t1 group by a order by a;
+------+---------------+
| a    | sum(floor(b)) |
+------+---------------+
|    1 |        0.0000 |
|    2 |        0.0000 |
|    3 |        0.0000 |
|    4 |       20.0000 |
|    5 |       20.0000 |
|    6 |       13.0000 |
|    7 |       -1.0000 |
|    8 |       -1.0000 |
|    9 |       -1.0000 |
|   10 |      -21.0000 |
|   11 |      -21.0000 |
|   12 |      -14.0000 |
+------+---------------+
```

## **限制**
MatrixOne目前只支持在查询表的时候使用函数，不支持单独使用函数。
