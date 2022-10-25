# 单表查询

本篇文章介绍如何使用 SQL 来对数据库中的数据进行查询。

## 开始前准备

你需要确认在开始之前，已经完成了以下任务：

- 已通过[源代码](https://docs.matrixorigin.io/cn/0.5.1/MatrixOne/Get-Started/install-standalone-matrixone/#1)或[二进制包](https://docs.matrixorigin.io/cn/0.5.1/MatrixOne/Get-Started/install-standalone-matrixone/#2)完成安装 MatrixOne
- 已完成[连接 MatrixOne 服务](../../Get-Started/connect-to-matrixone-server.md)

### 数据准备

#### 新建一个命名为 *token_count* 的表

```sql
CREATE TABLE token_count (
id int,
token varchar(100) DEFAULT '' NOT NULL,
count int DEFAULT 0 NOT NULL,
qty int,
phone char(1) DEFAULT '' NOT NULL,
times datetime DEFAULT '2000-01-01 00:00:00' NOT NULL
);
INSERT INTO token_count VALUES (21,'e45703b64de71482360de8fec94c3ade',3,7800,'n','1999-12-23 17:22:21');
INSERT INTO token_count VALUES (22,'e45703b64de71482360de8fec94c3ade',4,5000,'y','1999-12-23 17:22:21');
INSERT INTO token_count VALUES (18,'346d1cb63c89285b2351f0ca4de40eda',3,13200,'b','1999-12-23 11:58:04');
INSERT INTO token_count VALUES (17,'ca6ddeb689e1b48a04146b1b5b6f936a',4,15000,'b','1999-12-23 11:36:53');
INSERT INTO token_count VALUES (16,'ca6ddeb689e1b48a04146b1b5b6f936a',3,13200,'b','1999-12-23 11:36:53');
INSERT INTO token_count VALUES (26,'a71250b7ed780f6ef3185bfffe027983',5,1500,'b','1999-12-27 09:44:24');
INSERT INTO token_count VALUES (24,'4d75906f3c37ecff478a1eb56637aa09',3,5400,'y','1999-12-23 17:29:12');
INSERT INTO token_count VALUES (25,'4d75906f3c37ecff478a1eb56637aa09',4,6500,'y','1999-12-23 17:29:12');
INSERT INTO token_count VALUES (27,'a71250b7ed780f6ef3185bfffe027983',3,6200,'b','1999-12-27 09:44:24');
INSERT INTO token_count VALUES (28,'a71250b7ed780f6ef3185bfffe027983',3,5400,'y','1999-12-27 09:44:36');
INSERT INTO token_count VALUES (29,'a71250b7ed780f6ef3185bfffe027983',4,17700,'b','1999-12-27 09:45:05');
```

## 简单的查询

在 MySQL Client 等客户端输入并执行如下 SQL 语句：

```sql
> SELECT id, token FROM token_count;
```

输出结果如下：

```
+------+----------------------------------+
| id   | token                            |
+------+----------------------------------+
|   21 | e45703b64de71482360de8fec94c3ade |
|   22 | e45703b64de71482360de8fec94c3ade |
|   18 | 346d1cb63c89285b2351f0ca4de40eda |
|   17 | ca6ddeb689e1b48a04146b1b5b6f936a |
|   16 | ca6ddeb689e1b48a04146b1b5b6f936a |
|   26 | a71250b7ed780f6ef3185bfffe027983 |
|   24 | 4d75906f3c37ecff478a1eb56637aa09 |
|   25 | 4d75906f3c37ecff478a1eb56637aa09 |
|   27 | a71250b7ed780f6ef3185bfffe027983 |
|   28 | a71250b7ed780f6ef3185bfffe027983 |
|   29 | a71250b7ed780f6ef3185bfffe027983 |
+------+----------------------------------+
```

## 对结果进行筛选

如果你需要从诸多查询得到的结果中筛选出你需要的结果，可以通过 `WHERE` 语句对查询的结果进行过滤，从而找到想要查询的部分。

在 SQL 中，可以使用 `WHERE` 子句添加筛选的条件：

```sql
SELECT * FROM token_count WHERE id = 25;
```

输出结果如下：

```
+------+----------------------------------+-------+------+-------+---------------------+
| id   | token                            | count | qty  | phone | times               |
+------+----------------------------------+-------+------+-------+---------------------+
|   25 | 4d75906f3c37ecff478a1eb56637aa09 |     4 | 6500 | y     | 1999-12-23 17:29:12 |
+------+----------------------------------+-------+------+-------+---------------------+
```

## 对结果进行排序

使用 `ORDER BY` 语句可以让查询结果按照期望的方式进行排序。

例如，可以通过下面的 SQL 语句对 token_count 表的数据按照 times 列进行降序 (DESC) 排序。

```sql
SELECT id, token, times
FROM token_count
ORDER BY times DESC;
```

输出结果如下：

```
+------+----------------------------------+---------------------+
| id   | token                            | times               |
+------+----------------------------------+---------------------+
|   29 | a71250b7ed780f6ef3185bfffe027983 | 1999-12-27 09:45:05 |
|   28 | a71250b7ed780f6ef3185bfffe027983 | 1999-12-27 09:44:36 |
|   26 | a71250b7ed780f6ef3185bfffe027983 | 1999-12-27 09:44:24 |
|   27 | a71250b7ed780f6ef3185bfffe027983 | 1999-12-27 09:44:24 |
|   24 | 4d75906f3c37ecff478a1eb56637aa09 | 1999-12-23 17:29:12 |
|   25 | 4d75906f3c37ecff478a1eb56637aa09 | 1999-12-23 17:29:12 |
|   21 | e45703b64de71482360de8fec94c3ade | 1999-12-23 17:22:21 |
|   22 | e45703b64de71482360de8fec94c3ade | 1999-12-23 17:22:21 |
|   18 | 346d1cb63c89285b2351f0ca4de40eda | 1999-12-23 11:58:04 |
|   17 | ca6ddeb689e1b48a04146b1b5b6f936a | 1999-12-23 11:36:53 |
|   16 | ca6ddeb689e1b48a04146b1b5b6f936a | 1999-12-23 11:36:53 |
+------+----------------------------------+---------------------+
```

## 限制查询结果数量

如果希望只返回部分结果，可以使用 `LIMIT` 语句限制查询结果返回的记录数。

```sql
SELECT id, token, times
FROM token_count
ORDER BY times DESC
LIMIT 5;
```

运行结果如下：

```
+------+----------------------------------+---------------------+
| id   | token                            | times               |
+------+----------------------------------+---------------------+
|   29 | a71250b7ed780f6ef3185bfffe027983 | 1999-12-27 09:45:05 |
|   28 | a71250b7ed780f6ef3185bfffe027983 | 1999-12-27 09:44:36 |
|   26 | a71250b7ed780f6ef3185bfffe027983 | 1999-12-27 09:44:24 |
|   27 | a71250b7ed780f6ef3185bfffe027983 | 1999-12-27 09:44:24 |
|   24 | 4d75906f3c37ecff478a1eb56637aa09 | 1999-12-23 17:29:12 |
+------+----------------------------------+---------------------+
```

## 聚合查询

如果你想要关注数据整体的情况，而不是部分数据，你可以通过使用 `GROUP BY` 语句配合聚合函数，构建一个聚合查询来帮助你对数据的整体情况有一个更好的了解。

比如说，你可以将基本信息按照 id、count、times 列进行分组，然后分别统计：

```sql
SELECT id, count, times
FROM token_count
GROUP BY id, count, times
ORDER BY times DESC
LIMIT 5;
```

运行结果如下：

```
+------+-------+---------------------+
| id   | count | times               |
+------+-------+---------------------+
|   29 |     4 | 1999-12-27 09:45:05 |
|   28 |     3 | 1999-12-27 09:44:36 |
|   26 |     5 | 1999-12-27 09:44:24 |
|   27 |     3 | 1999-12-27 09:44:24 |
|   24 |     3 | 1999-12-23 17:29:12 |
+------+-------+---------------------+
```
