# **LPAD()**

## **函数说明**

函数LPAD(str,len,padstr)在字符串*str*左侧使用*padstr*进行填充，直至总长度为*len*的字符串，最后返回填充后的字符串。如果 *str*的长度大于*len*，那么最后的长度将缩减至*len*。  
若*len*为负数，则返回NULL。

## **函数语法**

```
> LPAD(str,len,padstr)
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
> SELECT Student_id, Student_name,LPAD(Student_Class, 10, ' _') AS LeftPaddedString FROM t1;
+------------+-----------------+------------------+
| Student_id | Student_name    | LeftPaddedString |
+------------+-----------------+------------------+
|          1 | Ananya Majumdar |  _ _ _ _IX       |
|          2 | Anushka Samanta |  _ _ _ _ X       |
|          3 | Aniket Sharma   |  _ _ _ _XI       |
|          4 | Anik Das        |  _ _ _ _ X       |
|          5 | Riya Jain       |  _ _ _ _IX       |
|          6 | Tapan Samanta   |  _ _ _ _ X       |
+------------+-----------------+------------------+
> SELECT Student_id, lpad(Student_name,4,'new') AS LeftPaddedString FROM t1;
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
> SELECT Student_id, lpad(Student_name,-4,'new') AS LeftPaddedString FROM t1;
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
> SELECT Student_id, lpad(Student_name,0,'new') AS LeftPaddedString FROM t1;
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
