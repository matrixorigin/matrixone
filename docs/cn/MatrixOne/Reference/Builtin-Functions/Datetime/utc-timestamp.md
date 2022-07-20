# **UTC_TIMESTAMP**

## **函数说明**

根据函数是否用在字符串或数字常数中，作为 *YYYY-MM-DD HH:MM:SS* 或 *YYYYMMDDHHMMSS* 格式的一个值，返回当前 UTC 日期和时间。

## **函数语法**

```
> UTC_TIMESTAMP()
```

## **示例**

```sql
> SELECT UTC_TIMESTAMP();
+---------------------+
| utc_timestamp()     |
+---------------------+
| 2022-06-22 22:31:13 |
+---------------------+
```
