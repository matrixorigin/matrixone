# **SUM**

## **函数说明**


`SUM()`聚合函数计算了一组值的和（NULL值被忽略）。



## **函数语法**

```
> SUM(expr)
```

## **参数释义**
|  参数  | 说明 |
|  ----  | ----  |
| expr  | 任何数值类型与字符串列的列名|

## **返回值**
返回`expr`列的数值的和，若输入参数为`Double`类型，则返回值为`Double`，否则为整数类型。  
如果没有匹配的行，则返回`NULL`值。

## **示例**


!!! note 提示
    `numbers(N)`是一个用于测试的数值表，仅有一列数据，包含了0至N-1的所有整数。


```sql
> SELECT SUM(*) FROM numbers(3);
+--------+
| sum(*) |
+--------+
|      3 |
+--------+

> SELECT SUM(number) FROM numbers(3);
+-------------+
| sum(number) |
+-------------+
|           3 |
+-------------+

> SELECT SUM(number) AS sum FROM numbers(3);
+------+
| sum  |
+------+
|    3 |
+------+

> SELECT SUM(number+2) AS sum FROM numbers(3);
+------+
| sum  |
+------+
|    9 |
+------+
```

***
