# MatrixOne 功能列表

本文档列出了 MatrixOne 最新版本所支持的功能。

## 数据定义语言（Data definition language, DDL）

| 数据定义语言(DDL) | 支持（Y）/不支持（N） |
| ----------------------------- | ---- |
| CREATE DATABASE               | Y    |
| RENAME DATABASE               | N    |
| DROP DATABASE                 | Y    |
| CREATE TABLE                  | Y    |
| ALTER TABLE                   | N    |
| MODIFY COLUMN                 | N    |
| RENAME TABLE                  | N    |
| PRIMARY KEY                   | Y    |
| CREATE VIEW                   | Y    |
| ALTER VIEW                    | N    |
| CREATE OR REPLACE VIEW        | N    |
| DROP VIEW                     | Y    |
| TRUNCATE                      | N    |
| SEQUENCE                      | N    |
| AUTO_INCREMENT                | Y    |
| Temporary tables              | Y    |

## SQL 语句

| SQL 语句                      | 支持（Y）/不支持（N）  |
| ----------------------------------- | ---- |
| SELECT                              | Y    |
| INSERT                              | Y    |
| UPDATE                              | Y    |
| DELETE                              | Y    |
| REPLACE                             | N    |
| INSERT ON DUPLICATE KEY             | N    |
| LOAD DATA INFILE                    | Y    |
| SELECT INTO OUTFILE                 | Y    |
| INNER/LEFT/RIGHT/OUTER JOIN         | Y    |
| UNION, UNION ALL                    | Y    |
| EXCEPT, INTERSECT                   | Y    |
| GROUP BY, ORDER BY                  | Y    |
| Common Table Expressions(CTE)       | Y    |
| START TRANSACTION, COMMIT, ROLLBACK | Y    |
| EXPLAIN                             | Y    |
| EXPLAIN ANALYZE                     | Y    |
| Stored Procedure                    | N    |
| Trigger                             | N    |
| Event Scheduler                     | N    |
| PARTITION BY                        | Y    |
| LOCK TABLE                          | Y    |

## 数据类型

| 数据类型分类 | 数据类型        | 支持（Y）/不支持（N）  |
| -------------------- | ----------------- | ---- |
| 整数类型      | TINYINT           | Y    |
|                      | SMALLINT          | Y    |
|                      | INT               | Y    |
|                      | BIGINT            | Y    |
|                      | TINYINT UNSIGNED  | Y    |
|                      | SMALLINT UNSIGNED | Y    |
|                      | INT UNSIGNED      | Y    |
|                      | BIGINT UNSIGNED   | Y    |
| 浮点类型         | FLOAT             | Y    |
|                      | DOUBLE            | Y    |
| 字符串类型         | CHAR              | Y    |
|                      | VARCHAR           | Y    |
|                      | TINYTEXT          | Y    |
|                      | TEXT              | Y    |
|                      | MEDIUMTEXT        | Y    |
|                      | LONGTEXT          | Y    |
| 二进制类型         | TINYBLOB          | Y    |
|                      | BLOB              | Y    |
|                      | MEDIUMBLOB        | Y    |
|                      | LONGBLOB          | Y    |
| 时间与日期  | Date              | Y    |
|                      | Time              | N    |
|                      | DateTime          | Y    |
|                      | Timestamp         | Y    |
| Boolean         | BOOL              | Y    |
| 定点类型         | DECIMAL           | Y    |
| JSON 类型            | JSON              | Y    |

## 索引与约束

| 索引与约束             | 支持（Y）/不支持（N）  |
| ------------------------------------ | ---- |
| 主键约束                          | Y    |
| 复合主键                | Y    |
| 唯一约束                           | Y    |
| Secondary KEY                        | Y    |
| 外键约束                          | N    |
| Enforced Constraints on Invalid Data | Y    |
| ENUM and SET Constraints             | N    |
| 非空约束                  | Y    |

## 事务

| 事务             | 支持（Y）/不支持（N）  |
| ------------------------ | ---- |
| 1PC                      | Y    |
| 悲观事务 | N    |
| 乐观事务  | Y    |
| 分布式事务  | Y    |
| 隔离级别       | Y    |

## 函数与操作符

| 函数与操作符 | 名称                |
| ---------------------------------- | ------------------- |
| 聚合函数                            | SUM()               |
|                                    | COUNT()             |
|                                    | MAX()               |
|                                    | MIN()               |
|                                    | AVG()               |
|                                    | STD()               |
|                                    | VARIANCE()          |
|                                    | BIT_OR()            |
|                                    | BIT_AND()           |
|                                    | BIT_XOR()           |
| 数学类                              | ABS()               |
|                                    | SIN()               |
|                                    | COS()               |
|                                    | TAN()               |
|                                    | COT()               |
|                                    | ACOS()              |
|                                    | ATAN()              |
|                                    | SINH()              |
|                                    | FLOOR()             |
|                                    | ROUND()             |
|                                    | CEIL()              |
|                                    | POWER()             |
|                                    | PI()                |
|                                    | LOG()               |
|                                    | LN()                |
|                                    | EXP()               |
| 日期时间类                          | DATE_FORMAT()       |
|                                    | YEAR()              |
|                                    | MONTH()             |
|                                    | DATE()              |
|                                    | WEEKDAY()           |
|                                    | DAYOFYEAR()         |
|                                    | EXTRACT()           |
|                                    | DATE_ADD()          |
|                                    | DATE_SUB()          |
|                                    | TO_DATE()           |
|                                    | DAY()               |
|                                    | UNIX_TIMESTAMP()    |
|                                    | FROM_UNIXTIME()     |
|                                    | UTC_TIMESTAMP()     |
|                                    | NOW()               |
|                                    | CURRENT_TIMESTAMP() |
|                                    | DATEDIFF()          |
|                                    | TIMEDIFF()          |
| 字符串类                            | BIN()               |
|                                    | CONCAT()            |
|                                    | CONCAT_WS()         |
|                                    | FIND_IN_SET()       |
|                                    | OCT()               |
|                                    | EMPTY()             |
|                                    | LENGTH()            |
|                                    | CHAR_LENGTH()       |
|                                    | LEFT()              |
|                                    | LTRIM()             |
|                                    | RTRIM()             |
|                                    | LPAD()              |
|                                    | RPAD()              |
|                                    | STARTSWITH()        |
|                                    | ENDSWITH()          |
|                                    | SUBSTRING()         |
|                                    | SPACE()             |
|                                    | TRIM                |
|                                    | REVERSE()           |
|                                    | UUID()              |
| 其他函数                            | COALESCE()          |
|                                    | ANY_VALUE()         |
|                                    | CAST()              |
| 操作符                              | =                   |
|                                    | <>                  |
|                                    | >                   |
|                                    | >=                  |
|                                    | <                   |
|                                    | <=                  |
|                                    | LIKE                |
|                                    | +                   |
|                                    | -                   |
|                                    | *                   |
|                                    | /                   |
|                                    | Div                 |
|                                    | %                   |
|                                    | AND                 |
|                                    | OR                  |
|                                    | XOR                 |
|                                    | NOT                 |
|                                    | CASE...WHEN         |
|                                    | IF                  |
|                                    | IS/IS NOT           |
|                                    | IS/IS NOT NULL      |
