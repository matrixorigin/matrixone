# **ABS()**

## **函数说明**

ABS(X)返回X的绝对值，或者NULL如果X是NULL.


## **函数语法**

```
> ABS(number)
```
## **参数释义**
|  参数   | 说明  |
|  ----  | ----  |
| number | 必要参数，可取任意数值数据类型 |

返回值类型与输入类型保持一致。

## **示例**

```sql
> drop table if exists t1;
> create table t1(a int,b float);
> insert into t1 values(1,-3.1416);
> insert into t1 values(-1,1.57);
> select abs(a),abs(b) from t1;
+--------+--------+
| abs(a) | abs(b) |
+--------+--------+
|      1 | 3.1416 |
|      1 | 1.5700 |
+--------+--------+
```

## **限制**
MatrixOne目前只支持在查询表的时候使用函数，不支持单独使用函数。
