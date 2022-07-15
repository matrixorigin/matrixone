# **ACOS()**

## **函数说明**

ACOS()函数返回给定数值的余弦（用弧度表示）。

## **函数语法**

```
> ACOS(number)
```

## **参数释义**

|  参数   | 说明  |
|  ----  | ----  |
| number | 必要参数，可取任意数值数据类型 |

## **示例**

```sql
> drop table if exists t1;
> create table t1(a float,b int);
> insert into t1 values(0.5,1);
> insert into t1 values(-0.5,-1);
> select acos(a),acos(b) from t1;
+---------+---------+
| acos(a) | acos(b) |
+---------+---------+
|  1.0472 |  0.0000 |
|  2.0944 |  3.1416 |
+---------+---------+

```
