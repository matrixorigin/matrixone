# **VARIANCE**

## **函数说明**

VAR(expr)是一个聚合函数，计算总体方差。

## **函数语法**

```
> VAR(expr)
```

## **参数释义**

|  参数   | 说明  |
|  ----  | ----  |
| expr  | 任何数值类型的列的列名 |

## **示例**

```sql
> CREATE TABLE t1(PlayerName VARCHAR(100) NOT NULL,RunScored INT NOT NULL,WicketsTaken INT NOT NULL);
> INSERT INTO t1 VALUES('KL Rahul', 52, 0 ),('Hardik Pandya', 30, 1 ),('Ravindra Jadeja', 18, 2 ),('Washington Sundar', 10, 1),('D Chahar', 11, 2 ),  ('Mitchell Starc', 0, 3);
> SELECT VAR(RunScored) as Pop_Standard_Variance FROM t1;
> SELECT VAR(WicketsTaken) as Pop_Std_Var_Wickets FROM t1;


> SELECT VAR(RunScored) as Pop_Standard_Deviation FROM t1;
+------------------------+
| Pop_Standard_Deviation |
+------------------------+
|               284.8056 |
+------------------------+
1 row in set (0.04 sec)

> SELECT VAR(WicketsTaken) as Pop_Std_Var_Wickets FROM t1;
+---------------------+
| Pop_Std_Var_Wickets |
+---------------------+
|              0.9167 |
+---------------------+
1 row in set (0.04 sec)

```
