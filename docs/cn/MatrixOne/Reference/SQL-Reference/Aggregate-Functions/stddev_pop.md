# **STDDEV_POP**

## **函数说明**



STDDEV_POP(expr)是一个聚合函数，计算总体标准差。


## **函数语法**

```
> STDDEV_POP(expr)
```
## **参数释义**
|  参数   | 说明  |
|  ----  | ----  |
| expr  | 任何数值类型的列的列名 |


## **示例**

```sql
> CREATE TABLE t1(PlayerName VARCHAR(100) NOT NULL,RunScored INT NOT NULL,WicketsTaken INT NOT NULL);
> INSERT INTO t1 VALUES('KL Rahul', 52, 0 ),('Hardik Pandya', 30, 1 ),('Ravindra Jadeja', 18, 2 ),('Washington Sundar', 10, 1),('D Chahar', 11, 2 ),  ('Mitchell Starc', 0, 3);
> SELECT STDDEV_POP(RunScored) as Pop_Standard_Deviation FROM t1;
> SELECT  STDDEV_POP(WicketsTaken) as Pop_Std_Dev_Wickets FROM t1;


> SELECT STDDEV_POP(RunScored) as Pop_Standard_Deviation FROM t1;
+------------------------+
| Pop_Standard_Deviation |
+------------------------+
|                16.8762 |
+------------------------+
1 row in set (0.02 sec)

> SELECT  STDDEV_POP(WicketsTaken) as Pop_Std_Dev_Wickets FROM t1;
+---------------------+
| Pop_Std_Dev_Wickets |
+---------------------+
|              0.9574 |
+---------------------+
1 row in set (0.01 sec)

```

## **限制**

MatrixOne目前只支持在查询表的时候使用函数，不支持单独使用函数。