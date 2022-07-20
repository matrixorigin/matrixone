# **RPAD()**

## **Description**

This function RPAD(str,len,padstr) returns the string *str*, right-padded with the string *padstr* to a length of *len* characters. If *str* is longer than *len*, the return value is shortened to *len* characters.

## **Syntax**

```
> RPAD(str,len,padstr)
```

## **Arguments**

|  Arguments   | Description  |
|  ----  | ----  |
| str | Required.  The string to be padded. CHAR and VARCHAR both are supported.|
| len | Required.  |
| padstr | Required. The string used to pad on the right. CHAR and VARCHAR both are supported.|

## **Examples**

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
