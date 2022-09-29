# **CURRENT_TIMESTAMP()**

## **函数说明**

`CURRENT_TIMESTAMP` 和 `CURRENT_TIMESTAMP()` 是 `NOW()` 的同义词。

将当前日期和时间以 `YYYY-MM-DD hh:mm:ss` 或 `YYYYMMDDhhmmss` 的格式返回，返回格式取决于函数是字符串还是数字。取值为当前会话所在的时区。

## **函数语法**

```
> CURRENT_TIMESTAMP([fsp])
```

## **Arguments**

|  参数   | 说明 |
|  ----  | ----  |
| fsp | 可选。参数 `fsp` 参数用于指定分秒精度，有效值为 0 到 6 之间的整数。 |

## **示例**

```sql
> SELECT CURRENT_TIMESTAMP();
+----------------------------+
| current_timestamp()        |
+----------------------------+
| 2022-09-21 11:46:44.153777 |
+----------------------------+
1 row in set (0.00 sec)

> SELECT NOW();
+----------------------------+
| now()                      |
+----------------------------+
| 2022-09-21 12:56:36.915961 |
+----------------------------+
1 row in set (0.01 sec)

> create table t1 (a int primary key, b int, c int, d timestamp default current_timestamp);
> insert into t1 select 1,1,1,now();
> insert into t1 select 2,0,0,null;
> select a,b,c,year(d) from t1;
+------+------+------+---------+
| a    | b    | c    | year(d) |
+------+------+------+---------+
|    1 |    1 |    1 |    2022 |
|    2 |    0 |    0 |    NULL |
+------+------+------+---------+
2 rows in set (0.01 sec)
```

## **限制**

运算符 `+` 或 `-` 现在不支持与 `CURRENT_TIMESTAMP` 一起使用。
