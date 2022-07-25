# **PI()**

## **函数说明**

PI() 返回数学常量π (pi).

## **函数语法**

```
> PI()
```

## **示例**

```sql
> drop table if exists t1;
> create table t1(a int,b float);
> insert into t1 values(0,0),(-15,-20),(-22,-12.5);
> insert into t1 values(0,360),(30,390),(90,450),(180,270),(180,180);
> select acos(a*pi()/180) as acosa,acos(b*pi()/180) acosb from t1;
+--------+--------+
| acosa  | acosb  |
+--------+--------+
| 1.5708 | 1.5708 |
| 1.8357 | 1.9274 |
| 1.9649 | 1.7907 |
| 1.5708 |   NULL |
| 1.0197 |   NULL |
|   NULL |   NULL |
|   NULL |   NULL |
|   NULL |   NULL |
+--------+--------+
> select acos(a*pi()/180)*acos(b*pi()/180) as acosab,acos(acos(a*pi()/180)) as c from t1;
+--------+------+
| acosab | c    |
+--------+------+
| 2.4674 | NULL |
| 3.5380 | NULL |
| 3.5186 | NULL |
|   NULL | NULL |
|   NULL | NULL |
|   NULL | NULL |
|   NULL | NULL |
|   NULL | NULL |
+--------+------+
```
