# **LPAD()**

## **函数说明**

函数LPAD(str,len,padstr)在字符串*str*左侧使用*padstr*进行填充，直至总长度为*len* characters，最后返回填充后的字符串。如果 *str*的长度大于*len*，那么最后的长度将缩减至*len*。  
若*len*为负数，则返回NULL。

## **函数语法**

```
> LPAD(str,len,padstr)
```

## **参数释义**
|  参数   | 说明  |
|  ----  | ----  |
| str |必要参数，被填充的字符串。CHAR与VARCHAR类型均可。|
| len |必要参数，  |
| padstr |必要参数，用于填充的字符串。CHAR与VARCHAR类型均可。|



## **示例**

```sql
> drop table if exists t1;
> create table t1(a varchar(10));
> insert into t1 values('mo');
> select a, lpad(a,8,'good') AS lpad1,lpad(a,1,'good') AS lpad2,lpad(a,-1,'good') AS lpad3 FROM t1;

+------+----------+-------+-------+
| a    | lpad1    | lpad2 | lpad3 |
+------+----------+-------+-------+
| mo   | goodgomo | m     | NULL  |
+------+----------+-------+-------+
```


## **限制**
MatrixOne目前只支持在查询表的时候使用函数，不支持单独使用函数。
