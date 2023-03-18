-- FUNCTION: mo_log_date
SELECT mo_log_date('2021/01/01') as date;
SELECT mo_log_date('2021/01/01') between '2021-01-01' and '2021-01-02' as val;
SELECT mo_log_date('2021/01/01') > '2021-01-01' as val;
SELECT mo_log_date('2021/01/01') < '2021-01-02' as val;
SELECT mo_log_date('2021-01-01') as date;
SELECT mo_log_date('/i/am/not/include/date/string') as date;
SELECT mo_log_date('/i/am/not/include/date/string') between '2021-01-01' and '2021-01-02' as val;
SELECT mo_log_date('etl:/sys/logs/2021/01/01/system.metric/filename') as date;
SELECT statement, mo_log_date(__mo_filepath) as date from system.statement_info where statement = 'IAMNOTSTATMENT';
-- check mix csv & tae
-- @bvt:issue#6812
CREATE EXTERNAL TABLE IF NOT EXISTS `statement_info`( `statement_id` VARCHAR(36) NOT NULL COMMENT "statement uniq id", `transaction_id` VARCHAR(36) NOT NULL COMMENT "txn uniq id", `session_id` VARCHAR(36) NOT NULL COMMENT "session uniq id", `account` VARCHAR(1024) NOT NULL COMMENT "account name", `user` VARCHAR(1024) NOT NULL COMMENT "user name", `host` VARCHAR(1024) NOT NULL COMMENT "user client ip", `database` VARCHAR(1024) NOT NULL COMMENT "what database current session stay in.", `statement` TEXT NOT NULL COMMENT "sql statement", `statement_tag` TEXT NOT NULL COMMENT "note tag in statement(Reserved)", `statement_fingerprint` TEXT NOT NULL COMMENT "note tag in statement(Reserved)", `node_uuid` VARCHAR(36) NOT NULL COMMENT "node uuid, which node gen this data.", `node_type` VARCHAR(1024) NOT NULL COMMENT "node type in MO, val in [DN, CN, LOG]", `request_at` Datetime(6) NOT NULL COMMENT "request accept datetime", `response_at` Datetime(6) NOT NULL COMMENT "response send datetime", `duration` BIGINT UNSIGNED DEFAULT "0" COMMENT "exec time, unit: ns", `status` VARCHAR(1024) NOT NULL COMMENT "sql statement running status, enum: Running, Success, Failed", `err_code` VARCHAR(1024) DEFAULT "0" COMMENT "error code info", `error` TEXT NOT NULL COMMENT "error message", `exec_plan` JSON NOT NULL COMMENT "statement execution plan", `rows_read` BIGINT DEFAULT "0" COMMENT "rows read total", `bytes_scan` BIGINT DEFAULT "0" COMMENT "bytes scan total", `stats` JSON NOT NULL COMMENT "global stats info in exec_plan", `statement_type` VARCHAR(1024) NOT NULL COMMENT "statement type, val in [Insert, Delete, Update, Drop Table, Drop User, ...]", `query_type` VARCHAR(1024) NOT NULL COMMENT "query type, val in [DQL, DDL, DML, DCL, TCL]", `role_id` BIGINT DEFAULT "0" COMMENT "role id", `sql_source_type` TEXT NOT NULL COMMENT "sql statement source type") infile{"filepath"="$resources/external_table_file/mix/*/*"} FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 0 lines;
-- @bvt:issue#6812
select count(statement_id) as cnt, mo_log_date(__mo_filepath) as date from statement_info group by __mo_filepath order by cnt;
-- @bvt:issue
create external table ex_table_2_18(num_col1 tinyint,num_col2 smallint,num_col3 int,num_col4 bigint,num_col5 tinyint unsigned,num_col6 smallint unsigned,num_col7 int unsigned,num_col8 bigint unsigned ,num_col9 float(5,3),num_col10 double(6,5),num_col11 decimal(38,19))infile{"filepath"='$resources/external_table_file/mix/*/*/*/ex_table_2_18.csv'} fields terminated by ',' enclosed by '\"' lines terminated by '\n';
select count(*) as cnt, mo_log_date(__mo_filepath) as date from ex_table_2_18 group by __mo_filepath order by cnt;
