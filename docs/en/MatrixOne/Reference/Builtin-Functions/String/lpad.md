# **LPAD()**

## **Description**

This function LPAD(str,len,padstr) returns the string *str*, left-padded with the string *padstr* to a length of *len* characters. If *str* is longer than *len*, the return value is shortened to *len* characters.

## **Syntax**

```
> LPAD(str,len,padstr)
```
## **Arguments**
|  Arguments   | Description  |
|  ----  | ----  |
| str | Required.  The string to be padded. CHAR and VARCHAR both are supported.|
| len | Required.  |
| padstr | Required. The string used to pad on the left. CHAR and VARCHAR both are supported.|



## **Examples**

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

## Constraints
Currently, MatrixOne doesn't support select function() without from tables.
