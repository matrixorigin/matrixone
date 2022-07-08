# **IS NOT**

## **语法说明**

`IS NOT` 运算符用于测试数值是否为布尔值，若不是布尔值，则返回结果为 `true`。其中 `boolean_value` 可以为 `TRUE`、`FALSE` 或 `UNKNOWN`。

## **语法结构**

```
> IS NOT boolean_value
```

## **示例**

```sql
> SELECT 1 IS NOT TRUE, 0 IS NOT FALSE, NULL IS NOT UNKNOWN;
+-----------+------------+----------+
| 1 != true | 0 != false | null !=  |
+-----------+------------+----------+
| false     | false      | NULL     |
+-----------+------------+----------+
1 row in set (0.01 sec)
```
