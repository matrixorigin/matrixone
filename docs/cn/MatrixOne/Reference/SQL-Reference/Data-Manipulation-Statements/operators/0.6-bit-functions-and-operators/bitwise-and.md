# **&**

## **运算符说明**

`Bitwise AND` 运算符，对每对比特位执行与（AND）操作。只有 a 和 b 都是 1 时，a AND b 才是 1。`Bitwise AND` 返回一个无符号的 64 位整数。

结果类型取决于参数是否被为二进制字符串或数字：

- 当参数为二进制字符串类型，并且其中至少一个不是十六进制 `literal`、位 `literal` 或 `NULL literal` 时，则进行二进制字符串求值计算；否则会进行数值求值计算，并根据需要将参数转换为无符号 64 位整数。

- 二进制字符串求值产生一个与参数长度相同的二进制字符串。如果参数的长度不相等，则会发生 `ER_INVALID_BITWISE_OPERANDS_SIZE` 错误。数值计算产生一个无符号的 64 位整数。

## **语法结构**

```
> SELECT value1 & value2;
```

## **示例**

```sql
> SELECT 29 & 15;
+---------+
| 29 & 15 |
+---------+
|      13 |
+---------+
1 row in set (0.06 sec)

> CREATE TABLE bitwise (a_int_value INT NOT NULL,b_int_value INT NOT NULL);
> INSERT bitwise VALUES (170, 75);  
> SELECT a_int_value & b_int_value FROM bitwise;  
+---------------------------+
| a_int_value & b_int_value |
+---------------------------+
|                        10 |
+---------------------------+
1 row in set (0.02 sec)
```
