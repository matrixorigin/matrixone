# **LTRIM()**

## **函数说明**

LTRIM()将输入字符串的前部空格去除，返回处理后的字符。

## **函数语法**

```
> LTRIM(char)
```

## **参数释义**

|  参数   | 说明  |
|  ----  | ----  |
| char | 必要参数，CHAR 与 VARCHAR均可|

## **示例**

```sql
> drop table if exists t1;
> create table t1(a char(8),b varchar(10));
> insert into t1 values('  matrix',' matrixone');
> select ltrim(a),ltrim(b) from t1;

+----------+-----------+
| ltrim(a) | ltrim(b)  |
+----------+-----------+
| matrix   | matrixone |
+----------+-----------+
```
