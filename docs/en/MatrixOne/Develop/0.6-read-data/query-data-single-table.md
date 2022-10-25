# Query Data from a Single Table

本篇文章介绍如何使用 SQL 来对数据库中的数据进行查询。

## Before you start

- Make sure you have already [installed and launched MatrixOne](https://docs.matrixorigin.io/0.5.1/MatrixOne/Get-Started/install-standalone-matrixone/).
- Use MySQL client to [connect to MatrixOne](https://docs.matrixorigin.io/0.5.1/MatrixOne/Get-Started/connect-to-matrixone-server/).

### Preparation

Create a tables named *token_count* to prepare for querying:

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

## Simple query

Execute the following SQL statement in a MySQL client:

```sql
> SELECT id, token FROM token_count;
```

Result is as below：

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

## Filter results

To filter query results, you can use the `WHERE` statement.

```sql
SELECT * FROM token_count WHERE id = 25;
```

Result is as below：

```
+------+----------------------------------+-------+------+-------+---------------------+
| id   | token                            | count | qty  | phone | times               |
+------+----------------------------------+-------+------+-------+---------------------+
|   25 | 4d75906f3c37ecff478a1eb56637aa09 |     4 | 6500 | y     | 1999-12-23 17:29:12 |
+------+----------------------------------+-------+------+-------+---------------------+
```

## Sort results

To sort query results, you can use the `ORDER BY` statement.

For example, the following SQL statement can be used to sort the data in the *token_count* table in descending order (DESC) by *times* column.

```sql
SELECT id, token, times
FROM token_count
ORDER BY times DESC;
```

Result is as below：

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

## Limit the number of query results

To limit the number of query results, you can use the `LIMIT` statement.

```sql
SELECT id, token, times
FROM token_count
ORDER BY times DESC
LIMIT 5;
```

Result is as below：

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

## Aggregate queries

To have a better understanding of the overall data situation, you can use the `GROUP BY` statement to aggregate query results.

For example, you can group basic information by `id`, `count`, and `times` columns and count them separately:

```sql
SELECT id, count, times
FROM token_count
GROUP BY id, count, times
ORDER BY times DESC
LIMIT 5;
```

Result is as below：

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
