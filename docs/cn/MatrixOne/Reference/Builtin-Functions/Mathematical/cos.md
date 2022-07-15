# **COS()**

## **函数说明**

COS()函数返回输入参数（用弧度表示）的余弦值。

## **函数语法**

```
> COS(number)
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
> insert into t1 values(-1,1.57);
> select cos(a),cos(b) from t1;
+--------+---------+
| cos(a) | cos(b)  |
+--------+---------+
| 0.5403 | -1.0000 |
| 0.5403 |  0.0008 |
+--------+---------+
```
