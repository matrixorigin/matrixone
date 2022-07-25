# **SPACE()**

## **函数说明**

SPACE(N)返回N个空格组成的字符串。

## **语法**

```
> SPACE(N)
```

## **参数释义**

|  参数   | 说明  |
|  ----  | ----  |
| N | 必要参数. UINT类型 |

## **示例**

```SQL
> drop table if exists t1;
> CREATE TABLE t1
(
Employee_name VARCHAR(100) NOT NULL,
Joining_Date DATE NOT NULL
);
> INSERT INTO t1
(Employee_name, Joining_Date )
VALUES
('     Ananya Majumdar', '2000-01-11'),
('   Anushka Samanta', '2002-11-10' ),
('   Aniket Sharma ', '2005-06-11' ),
('   Anik Das', '2008-01-21'  ),
('  Riya Jain', '2008-02-01' ),
('    Tapan Samanta', '2010-01-11' ),
('   Deepak Sharma', '2014-12-01'  ),
('   Ankana Jana', '2018-08-17'),
('  Shreya Ghosh', '2020-09-10') ;
> INSERT INTO t1
(Employee_name, Joining_Date ) values('     ','2014-12-01');
> select * from t1 where Employee_name=space(5);
+---------------+--------------+
| Employee_name | Joining_Date |
+---------------+--------------+
|               | 2014-12-01   |
+---------------+--------------+
```
