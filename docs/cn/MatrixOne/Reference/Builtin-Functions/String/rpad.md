# **RPAD()**

## **函数说明**

函数RPAD(str,len,padstr)在字符串*str*右侧使用*padstr*进行填充，直至总长度为*len*的字符串，最后返回填充后的字符串。如果 *str*的长度大于*len*，那么最后的长度将缩减至*len*。  
若*len*为负数，则返回NULL。

## **函数语法**

```
> RPAD(str,len,padstr)
```

## **参数释义**
|  参数   | 说明  |
|  ----  | ----  |
| str |必要参数，被填充的字符串。CHAR与VARCHAR类型均可。|
| len |必要参数，需要填充到的总长度。  |
| padstr |必要参数，用于填充的字符串。CHAR与VARCHAR类型均可。|



## **示例**

```sql
> drop table if exists t1;
> CREATE TABLE t1(Student_id INT,Student_name VARCHAR(100),Student_Class CHAR(20));
> INSERT INTO t1
VALUES
(1,'Ananya Majumdar', 'IX'),
(2,'Anushka Samanta', 'X'),
(3,'Aniket Sharma', 'XI'),
(4,'Anik Das', 'X'),
(5,'Riya Jain', 'IX'),
(6,'Tapan Samanta', 'X');
> SELECT Student_id, Student_name,RPAD(Student_Class, 10, ' _') AS LeftPaddedString FROM t1;
+------------+-----------------+------------------+
| Student_id | Student_name    | LeftPaddedString |
+------------+-----------------+------------------+
|          1 | Ananya Majumdar | IX _ _ _ _       |
|          2 | Anushka Samanta | X _ _ _ _        |
|          3 | Aniket Sharma   | XI _ _ _ _       |
|          4 | Anik Das        | X _ _ _ _        |
|          5 | Riya Jain       | IX _ _ _ _       |
|          6 | Tapan Samanta   | X _ _ _ _        |
+------------+-----------------+------------------+
> SELECT Student_id, rpad(Student_name,4,'new') AS LeftPaddedString FROM t1;
+------------+------------------+
| Student_id | LeftPaddedString |
+------------+------------------+
|          1 | Anan             |
|          2 | Anus             |
|          3 | Anik             |
|          4 | Anik             |
|          5 | Riya             |
|          6 | Tapa             |
+------------+------------------+
> SELECT Student_id, rpad(Student_name,-4,'new') AS LeftPaddedString FROM t1;
+------------+------------------+
| Student_id | LeftPaddedString |
+------------+------------------+
|          1 | NULL             |
|          2 | NULL             |
|          3 | NULL             |
|          4 | NULL             |
|          5 | NULL             |
|          6 | NULL             |
+------------+------------------+
> SELECT Student_id, rpad(Student_name,0,'new') AS LeftPaddedString FROM t1;
+------------+------------------+
| Student_id | LeftPaddedString |
+------------+------------------+
|          1 |                  |
|          2 |                  |
|          3 |                  |
|          4 |                  |
|          5 |                  |
|          6 |                  |
+------------+------------------+
```


## **限制**
MatrixOne目前只支持在查询表的时候使用函数，不支持单独使用函数。
