# **REVERSE()**

## **Description**

Returns the string str with the order of the characters reversed.

## **Syntax**

```
> REVERSE(str)
```

## **Arguments**

|  Arguments   | Description  |
|  ----  | ----  |
| Str | Required. both CHAR and VARCHAR are supported. |

## **Examples**

```SQL
> drop table if exists t1;
> create table t1(a varchar(12),c char(30));
> insert into t1 values('sdfad  ','2022-02-02 22:22:22');
> insert into t1 values('  sdfad  ','2022-02-02 22:22:22');
> insert into t1 values('adsf  sdfad','2022-02-02 22:22:22');
> insert into t1 values('    sdfad','2022-02-02 22:22:22');
> select reverse(a),reverse(c) from t1;
+-------------+---------------------+
| reverse(a)  | reverse(c)          |
+-------------+---------------------+
|   dafds     | 22:22:22 20-20-2202 |
|   dafds     | 22:22:22 20-20-2202 |
| dafds  fsda | 22:22:22 20-20-2202 |
| dafds       | 22:22:22 20-20-2202 |
+-------------+---------------------+
> select a from t1 where reverse(a) like 'daf%';
+-------------+
| a           |
+-------------+
| adsf  sdfad |
|     sdfad   |
+-------------+
> select reverse(a) reversea,reverse(reverse(a)) normala from t1;
+-------------+-------------+
| reversea    | normala     |
+-------------+-------------+
|   dafds     | sdfad       |
|   dafds     |   sdfad     |
| dafds  fsda | adsf  sdfad |
| dafds       |     sdfad   |
+-------------+-------------+
```
