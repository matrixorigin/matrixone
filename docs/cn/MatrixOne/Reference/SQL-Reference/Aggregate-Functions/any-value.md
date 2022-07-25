# **ANY_VALUE**

## **函数说明**

`ANY_VALUE` 在范围内任选一个值返回。

## **函数语法**

```
> ANY_VALUE(arg)
```

## **参数释义**

|  参数  | 说明 |
|  ----  | ----  |
| arg  | 可为任意类型。当 arg 为 NULL 时，该行不参与计算。|

## **返回值**

返回类型和输入类型相同。

说明：`ANY_VALUE` 的执行结果具有不确定性，相同的输入可能得到不同的执行结果。

## **示例**

```sql
> create table t1(
    -> a int,
    -> b int,
    -> c int
    -> );
> create table t2(
    -> a int,
    -> b int,
    -> c int
    -> );
> insert into t1 values(1,10,34),(2,20,14);
> insert into t2 values(1,-10,-45);
> select ANY_VALUE(t1.b) from t1 left join t2 on t1.c=t1.b and t1.a=t1.c group by t1.a;
+-----------------+
| any_value(t1.b) |
+-----------------+
|              10 |
|              20 |
+-----------------+
2 rows in set (0.01 sec)
> select 3+(5*ANY_VALUE(t1.b)) from t1 left join t2 on t1.c=t1.b and t1.a=t1.c group by t1.a;
+---------------------------+
| 3 + (5 * any_value(t1.b)) |
+---------------------------+
|                        53 |
|                       103 |
+---------------------------+
2 rows in set (0.00 sec)
```
