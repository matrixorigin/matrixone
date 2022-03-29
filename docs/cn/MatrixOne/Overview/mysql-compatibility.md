# **MySQL兼容性** 

MatrixOne的SQL语法兼容了MySQL 8.0.23版本。


|  语句类型   | 语法 |  兼容性  |
|  ----  | ----  |  ----  |
| DDL  | CREATE DATABASE | 以中文作为表名会导致显示错误 | 
|   |   | 支持部分拉丁语  | 
|   |   | `ENCRYPTION`目前可以使用但无效|
|   | CREATE TABLE | 不单独支持临时表 | 
|   |   | 不支持表的分区 | 
|   |   | 不支持`Create table as` 语句|
|   |   | 不支持列级约束|
|   |   | 支持`DEFAULT` |
|   |   | 对于聚簇表，在DDL语句末尾应该加上("bucket"="n") |
|   |   | 不支持`KEY(column)`语法|
|   | CREATE other projects |暂不支持  | 
|   | ALTER | 暂不支持  | 
|   | DROP DATABASE | 同MySQL | 
|   | DROP TABLE | 同MySQL | 
|   | Drop Other objects | 只支持`DROP INDEX`|
| DML  | INSERT | 现不支持`LOW_PRIORITY`，`DELAYED`，`HIGH_PRIORITY`| 
|   |   | 不支持使用`select`来插入| 
|   |   | 批处理`Insert`不超过5000行| 
|   |   | 暂不支持`ON DUPLICATE KEY UPDATE`| 
|   |   | 不支持`DELAYED` | 
|   |   | 不支持`HAVING`语句| 
|   |   | 支持部分拉丁语  | 
|   |   | 当前模式与MySQL的`only_full_group_by`相同  | 
|   | DELETE | 暂不支持  | 
|   | UPDATE | 暂不支持 | 
|   | SELECT | 在多表查询时，只支持带有`GROUP BY`与`ORDER BY`语句的`INNER JOIN`的命令| 
|   |   | `INNER JOIN`中不支持表的别名  | 
|   |   | 不支持子查询  | 
|   |   | 部分支持`Distinct`  | 
|   |   | 不支持`For`语句  | 
|   |   | 部分支持`INTO OUTFILE` | 
|   | LOAD DATA | 只能导入csv文件   | 
|   |   | 包括符`enclosed`应该为`""`|
|   |   | 字段分隔符`FILEDS TERMINATED BY`应该为 `,` 或 `|` | 
|   |   | 行分隔符`LINES TERMINATED BY`应该为`\n` | 
|   |   | 不支持`SET` | 
|   |   | 不支持本地关键词 | 
|   |   | 只有mo-server上的文件才支持相对路径 | 
| 数据库管理语句  | SHOW |只支持显示数据库与数据表  | 
|   |  | 支持`SHOW CREATE TABLE` 和`SHOW CREATE DATABASE` ｜
|   |  | 部分支持`WHERE`语句  | 
|   | 其他语法| 暂不支持  |
| 工具类语句  | USE | `Use database`同MySQL  | 
|   | Explain | 分析的结果与MySQL有所不同 | 
|   | Other statements |暂不支持  | 
| 数据类型  | Int/Bigint/Smallint/Tinyint | 同MySQL  | 
|   | char/varchar | 同MySQL  | 
|   | Float/double | 与MySQL的精度有所不同，将在未来版本调整|
|   | Date | 只支持`YYYY-MM-DD'`与`YYYYMMDD`形式 | 
|   | Datetime | 只支持`YYYY-MM-DD HH:MM:SS` 与 `YYYYMMDD HH:MM:SS`形式  | 
|   | Other types | 暂不支持  | 
| 运算符  | "+","-","*","/" | 同MySQL  | 
|   | DIV, %, MOD | 暂不支持 | 
|   | LIKE | 部分支持  | 
|   | IN | 只支持常数列表 | 
|   | NOT, AND, &&,OR, "\|\|" | 同MySQL  | 
|   | XOR | 暂不支持 | 
|   | MAX, MIN, COUNT, AVG | 同MySQL  | 
|   | CAST | 部分支持 | 






