# **STDDEV_POP**

## **Description**

Aggregate function.

The STDDEV_POP(expr) function returns the population standard deviation of expr.

## **Syntax**

```
> STDDEV_POP(expr)
```

## **Arguments**

|  Arguments   | Description  |
|  ----  | ----  |
| expr  | Any numerical expressions |

## **Examples**

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
