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
|   | CREATE other projects | Not supported now.  | 
|   | ALTER | Not supported now.  | 
|   | DROP DATABASE | Same as MySQL. | 
|   | DROP TABLE | Same as MySQL. | 
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
|   | SELECT | All multi tables queries are not supported now. | 
|   |   | Sub query is not supported now.  | 
|   |   | Distinct is limitedly support.  | 
|   |   | For clause is not supported now.  | 
|   |   | INTO OUTFILE is not supported now. | 
|   | LOAD DATA | Only csv files can be loaded currently.  | 
|   |   | The enclosed character shoud be "".  | 
|   |   | FILEDS TERMINATED BY should be "," or "|". | 
|   |   | LINES TERMINATED BY should be "\n". | 
|   |   | SET is not supported now. | 
|   |   | Local key word is not supported now. | 
| Database Administration Statements  | SHOW | Only show tables and show databases are supported.  | 
|   |  | Where can be supported limitedly.  | 
|   | Other statements | Not supported now.  |
| Utility Statements  | USE | Use database is the same as MySQL.  | 
|   | Explain | The result of explain a SQL is quite different with MySQL. | 
|   | Other statements | Not supported now.  | 
| Data Types  | Int/Bigint/Smallint/Tinyint | Same as MySQL.  | 
|   | char/varchar | Same as MySQL.  | 
|   | Float/double | The precsion is a bit different with MySQL. It will be adjusted in future release.  | 
|   | Date | Not supported now.  | 
|   | Other types | Not supported now.  | 
| Operatiors  | "+","-","*","/" | Same as MySQL.  | 
|   | DIV, %, MOD | Not supported now.  | 
|   | LIKE, IN | Not supported now.  | 
|   | NOT, AND, &&,OR, "\|\|" | Same as MySQL.  | 
|   | XOR | Not supported now.  | 
|   | MAX, MIN, COUNT, AVG | Same as MySQL.  | 
|   | CAST | Supported limitedly.  | 






