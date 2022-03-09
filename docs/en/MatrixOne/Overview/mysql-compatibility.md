# **MySQL Compatibility** 

MatrixOne SQL syntax conforms with MySQL 8.0.23 version. 



|  SQL Type   | SQL Syntax  |  Compability with MySQL8.0.23   |
|  ----  | ----  |  ----  |
| DDL  | CREATE DATABASE | A database with Chinese name will be displayed incorrectly.  | 
|   |   | Names with Latins support limitedly.  | 
|   |   | ENCRYPTION can be used but doesn't work till now. |
|   | CREATE TABLE | Temporary tables are not supported seperatedly.  | 
|   |   | Partition tables are not supported.  | 
|   |   | Create table .. as clause is not supported now. |
|   |   | All column level constraints are not supported now. |
|   |   | DEFAULT can be supported now. |
|   |   | For cluster table, there should be properties("bucket"="n") in the end of DDL. |
|   |   | KEY(column) is not supported yet.|
|   | CREATE other projects | Not supported now.  | 
|   | ALTER | Not supported now.  | 
|   | DROP DATABASE | Same as MySQL. | 
|   | DROP TABLE | Same as MySQL. | 
|   | Drop Other objects | Only DROP INDEX is supported. |
| DML  | INSERT | LOW_PRIORITY, DELAYED, HIGH_PRIORITY are not supported now.  | 
|   |   | Insert with select is not supported now. | 
|   |   | Batch Insert can be supported less than 5,000 rows.  | 
|   |   | ON DUPLICATE KEY UPDATE is not supported  now.  | 
|   |   | DELAYED is not supported now.  | 
|   |   | HAVING clause is not supported now. | 
|   |   | Names with Latins support limitedly.  | 
|   |   | The current mode is just like only_full_group_by mode in MySQL.  | 
|   | DELETE | Not supported now.  | 
|   | UPDATE | Not supported now.  | 
|   | SELECT | Only INNER JOIN with GROUP BY and ORDER BY is supported in multi table scenarios. | 
|   |   | Table alias is not supported in INNER JOIN.  | 
|   |   | Sub query is not supported now.  | 
|   |   | Distinct is limitedly support.  | 
|   |   | For clause is not supported now.  | 
|   |   | INTO OUTFILE is limitedly support. | 
|   | LOAD DATA | Only csv files can be loaded currently.  | 
|   |   | The enclosed character shoud be "".  | 
|   |   | FILEDS TERMINATED BY should be "," or "|". | 
|   |   | LINES TERMINATED BY should be "\n". | 
|   |   | SET is not supported now. | 
|   |   | Local key word is not supported now. | 
|   |   | Relative path is limited supported now. Only based on mo-server file can be supported. | 
| Database Administration Statements  | SHOW | Only show tables and show databases are supported.  | 
|   |  | Show CREATE TABLE and CREATE DATABASE are supported.  |
|   |  | Where can be supported limitedly.  | 
|   | Other statements | Not supported now.  |
| Utility Statements  | USE | Use database is the same as MySQL.  | 
|   | Explain | The result of explain a SQL is quite different with MySQL. | 
|   | Other statements | Not supported now.  | 
| Data Types  | Int/Bigint/Smallint/Tinyint | Same as MySQL.  | 
|   | char/varchar | Same as MySQL.  | 
|   | Float/double | The precsion is a bit different with MySQL. It will be adjusted in future release.  | 
|   | Date | Only 'YYYY-MM-DD' and 'YYYYMMDD' formats are supported.  | 
|   | Datetime | Only 'YYYY-MM-DD HH:MM:SS' and 'YYYYMMDD HH:MM:SS' formats are supported.  | 
|   | Other types | Not supported now.  | 
| Operatiors  | "+","-","*","/" | Same as MySQL.  | 
|   | DIV, %, MOD | Not supported now.  | 
|   | LIKE | Supported with constraints.  | 
|   | IN | Supported for constant lists  | 
|   | NOT, AND, &&,OR, "\|\|" | Same as MySQL.  | 
|   | XOR | Not supported now.  | 
|   | MAX, MIN, COUNT, AVG | Same as MySQL.  | 
|   | CAST | Supported limitedly.  | 






