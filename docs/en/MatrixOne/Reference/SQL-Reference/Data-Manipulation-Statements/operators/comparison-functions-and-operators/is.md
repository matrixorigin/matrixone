# **IS**

## **Description**

The `IS` tests a value against a boolean value, where boolean_value can be TRUE, FALSE, or UNKNOWN.

## **Syntax**

```
> IS boolean_value
```

## **Examples**

```sql
> SELECT 1 IS TRUE, 0 IS FALSE, NULL IS UNKNOWN;
+----------+-----------+---------+
| 1 = true | 0 = false | null =  |
+----------+-----------+---------+
| true     | true      | NULL    |
+----------+-----------+---------+
1 row in set (0.01 sec)
```

```sql
> create table t1 (a boolean,b bool);
> insert into t1 values (0,1),(true,false),(true,1),(0,false),(NULL,NULL);
> select * from t1;
+-------+-------+
| a     | b     |
+-------+-------+
| false | true  |
| true  | false |
| true  | true  |
| false | false |
| NULL  | NULL  |
+-------+-------+
> select * from t1 where a<=b and a is not NULL;
+-------+-------+
| a     | b     |
+-------+-------+
| false | true  |
| true  | true  |
| false | false |
+-------+-------+
3 rows in set (0.01 sec)
```
