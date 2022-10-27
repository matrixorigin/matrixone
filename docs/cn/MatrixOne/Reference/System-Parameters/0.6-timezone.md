# 时区支持

MatrixOne 使用的时区取决于全局变量 `time_zone`。`time_zone` 的默认值是 `System`。

你可以使用如下命令修改全局时区：

```
> SET GLOBAL time_zone = timezone;
```

使用以下 SQL 语句查看当前全局时区、客户端时区和系统时区的值：

```sql
> SELECT @@global.time_zone, @@session.time_zone, @@global.system_time_zone;
+-------------+-------------+--------------------+
| @@time_zone | @@time_zone | @@system_time_zone |
+-------------+-------------+--------------------+
| timezone    | +08:00      |                    |
+-------------+-------------+--------------------+
1 row in set (0.00 sec)
```

设置 time_zone 的值的格式：

`SYSTEM` 表明使用系统时间
相对于 UTC 时间的偏移，比如 `+10:00` 或者 `-6:00`
某个时区的名字，比如 `Europe/Helsinki`，`US/Eastern` 或 `MET`
NOW() 和 CURTIME() 的返回值都受到时区设置的影响。

!!! note
    只有 Timestamp 数据类型的值是受时区影响的。可以理解为，Timestamp 数据类型的实际表示使用的是（字面值 + 时区信息）。其它时间和日期类型，比如 Datetime/Date/Time 是不包含时区信息的，所以也不受到时区变化的影响。

```sql
> create table t (ts timestamp, dt datetime);
Query OK, 0 rows affected (0.02 sec)

mysql> set @@time_zone = 'UTC';
Query OK, 0 rows affected (0.00 sec)

mysql> insert into t values ('2017-09-30 11:11:11', '2017-09-30 11:11:11');
Query OK, 1 row affected (0.02 sec)

mysql> set @@time_zone = '+8:00';
ERROR 20101 (HY000): internal error: incorrect timezone +8:00
mysql> set @@time_zone = '+08:00';
Query OK, 0 rows affected (0.00 sec)

mysql> select * from t;
+---------------------+---------------------+
| ts                  | dt                  |
+---------------------+---------------------+
| 2017-09-30 19:11:11 | 2017-09-30 11:11:11 |
+---------------------+---------------------+
1 row in set (0.00 sec)
```

上面的例子中，无论怎么调整时区的值，Datetime 类型字段的值是不受影响的，而 Timestamp 则随着时区改变，显示的值会发生变化。其实 Timestamp 持久化到存储的值始终没有变化过，只是根据时区的不同显示值不同。

!!! note
    Timestamp 类型和 Datetime 等类型的值，两者相互转换的过程中，会涉及到时区。这种情况一律基于当前 time_zone 时区处理。

## 修改 MatrixOne 时区

1. 查看当前时间或时区：

```sql
> select now();
+----------------------------+
| now()                      |
+----------------------------+
| 2022-10-14 18:38:27.876181 |
+----------------------------+
1 row in set (0.00 sec)


> show variables like "%time_zone%";
+------------------+--------+
| Variable_name    | Value  |
+------------------+--------+
| system_time_zone |        |
| time_zone        | +08:00 |
+------------------+--------+
2 rows in set (0.00 sec)
```

- `time_zone`：使用system的时区。

- `system_time_zone` 说明system使用CST时区。

2. 修改当前时区：

```
set global time_zone = '+08:00';
set time_zone = '+08:00';
```

- `set global time_zone = '+08:00';`：修改mysql全局时区为北京时间，即我们所在的东8区。
- `set time_zone = '+08:00';`：修改当前会话时区。
