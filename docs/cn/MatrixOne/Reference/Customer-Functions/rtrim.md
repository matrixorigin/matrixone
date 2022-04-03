# **RTRIM()**

## **函数说明**

RTRIM()将输入字符串的后方空格去除，返回处理后的字符。

## **函数语法**

```
> RTRIM(char)
```
## **参数释义**
|  参数   | 说明  |
|  ----  | ----  |
| char | 必要参数，CHAR 与 VARCHAR均可|



## **示例**

```sql
> drop table if exists t1;
> create table t1(a char(8),b varchar(10));
> insert into t1 values('matrix  ','matrixone ');
> select rtrim(a),rtrim(b) from t1;

+----------+-----------+
| rtrim(a) | rtrim(b)  |
+----------+-----------+
| matrix   | matrixone |
+----------+-----------+
```

## **限制**
MatrixOne目前只支持在查询表的时候使用函数，不支持单独使用函数。
