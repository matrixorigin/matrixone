# MatrixOne Features

This document lists the features supported by MatrixOne for the latest version.

## Data definition language (DDL)

| Data definition Language(DDL) | Supported（Y）/Not support（N）  |
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

## SQL statements

| SQL Statement                       | Supported（Y）/Not support（N）  |
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

## Data types

| Data type categories | Data types        | Supported（Y）/Not support（N）  |
| -------------------- | ----------------- | ---- |
| Integer Numbers      | TINYINT           | Y    |
|                      | SMALLINT          | Y    |
|                      | INT               | Y    |
|                      | BIGINT            | Y    |
|                      | TINYINT UNSIGNED  | Y    |
|                      | SMALLINT UNSIGNED | Y    |
|                      | INT UNSIGNED      | Y    |
|                      | BIGINT UNSIGNED   | Y    |
| Real Numbers         | FLOAT             | Y    |
|                      | DOUBLE            | Y    |
| String Types         | CHAR              | Y    |
|                      | VARCHAR           | Y    |
|                      | TINYTEXT          | Y    |
|                      | TEXT              | Y    |
|                      | MEDIUMTEXT        | Y    |
|                      | LONGTEXT          | Y    |
| Binary Types         | TINYBLOB          | Y    |
|                      | BLOB              | Y    |
|                      | MEDIUMBLOB        | Y    |
|                      | LONGBLOB          | Y    |
| Time and Date Types  | Date              | Y    |
|                      | Time              | N    |
|                      | DateTime          | Y    |
|                      | Timestamp         | Y    |
| Boolean Type         | BOOL              | Y    |
| Decimal Type         | DECIMAL           | Y    |
| JSON Type            | JSON              | Y    |

## Indexing and constraints

| Indexing and constraints             | Supported（Y）/Not support（N）  |
| ------------------------------------ | ---- |
| PRIMARY KEY                          | Y    |
| Composite PRIMARY KEY                | Y    |
| UNIQUE KEY                           | Y    |
| Secondary KEY                        | Y    |
| FOREIGN KEY                          | N    |
| Enforced Constraints on Invalid Data | Y    |
| ENUM and SET Constraints             | N    |
| NOT NULL Constraint                  | Y    |

## Transactions

| Transactions             | Supported（Y）/Not support（N）  |
| ------------------------ | ---- |
| 1PC                      | Y    |
| Pessimistic transactions | N    |
| Optimistic transactions  | Y    |
| Distributed Transaction  | Y    |
| Snapshot Isolation       | Y    |

## Functions and Operators

| Functions and Operators Categories | Name                |
| ---------------------------------- | ------------------- |
| Aggregate functions                | SUM()               |
|                                    | COUNT()             |
|                                    | MAX()               |
|                                    | MIN()               |
|                                    | AVG()               |
|                                    | STD()               |
|                                    | VARIANCE()          |
|                                    | BIT_OR()            |
|                                    | BIT_AND()           |
|                                    | BIT_XOR()           |
| Mathematical functions             | ABS()               |
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
| Datetime functions                 | DATE_FORMAT()       |
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
| String functions                   | BIN()               |
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
| Other functions                    | COALESCE()          |
|                                    | ANY_VALUE()         |
|                                    | CAST()              |
| Operators                          | =                   |
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
