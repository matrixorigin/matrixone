# **TAN()**

## **函数说明**

TAN()函数返回输入参数（用弧度表示）的正切值。

## **函数语法**

```
> TAN(number)
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
> insert into t1 values(-1,-3.14159);
> select tan(a),tan(b) from t1;
+---------+---------+
| tan(a)  | tan(b)  |
+---------+---------+
|  1.5574 | -0.0000 |
| -1.5574 |  0.0000 |
+---------+---------+
```
