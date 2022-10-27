# **SHOW DATABASES**

## **函数说明**

`SHOW DATABASES` 列出 MatrixOne上的数据库。`SHOW SCHEMAS` 是 `SHOW DATABASES` 的同义词。

如果存在 `LIKE` 子句，表示需要匹配哪些数据库名。`WHERE` 子句可以使用通用的条件来选择行。

MatrixOne 将数据库展示在数据目录中。

数据库信息也可以从 `INFORMATION_SCHEMA` SCHEMATA 表中获得。

## **函数语法**

```
> SHOW {DATABASES | SCHEMAS}
    [LIKE 'pattern' | WHERE expr]
```

## **示例**

```sql
> create database demo_1;
> show databases;
+--------------------+
| Database           |
+--------------------+
| mo_catalog         |
| system             |
| system_metrics     |
| information_schema |
| demo_1             |
+--------------------+
7 rows in set (0.00 sec)
