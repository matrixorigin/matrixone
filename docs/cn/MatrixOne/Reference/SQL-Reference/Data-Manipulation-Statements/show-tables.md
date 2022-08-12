# **SHOW TABLES**

## **语法说明**

以列表的形式展现当前数据库创建的所有表。

## **语法结构**

```
> SHOW TABLES  [LIKE 'pattern' | WHERE expr | FROM 'pattern' | IN 'pattern']
```

## **示例**

```
> SHOW TABLES;
+---------------+
| name          |
+---------------+
| clusters      |
| contributors  |
| databases     |
| functions     |
| numbers       |
| numbers_local |
| numbers_mt    |
| one           |
| processes     |
| settings      |
| tables        |
| tracing       |
+---------------+
```

## **限制**

MatrixOne 暂不支持 `SHOW TABLE STATUS`。
