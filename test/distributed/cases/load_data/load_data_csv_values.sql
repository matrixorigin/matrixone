-- test load data, integer numbers
drop table if exists t1;
create table t1(
col1 tinyint
);

-- load data
load data inline format='csv', data='1\n2\n' into table t1 fields terminated by ',';
load data  inline format='csv', data=$XXX$
1
2
$XXX$ 
into table t1;
select * from t1;

-- test load data, text
drop table if exists t1;
create table t1(
col1 text
);

-- load data
load data inline format='csv', data='"1
 2"\n"2"\n' into table t1;

select * from t1;

-- test load data, Time and Date type
drop table if exists t4;
create table t4(
col1 date,
col2 datetime,
col3 timestamp,
col4 bool
);

-- load data
load data inline format='csv', data='1000-01-01,0001-01-01,1970-01-01 00:00:01,0
9999-12-31,9999-12-31,2038-01-19,1
' into table t4;
select * from t4;


create table t5(
col1 text
);
load data inline format='jsonline', data='{"col1":"good"}' , jsontype = 'object'  into table t5;
load data inline format='unknow', data='{"col1":"good"}' , jsontype = 'object'  into table t5;

CREATE TABLE IF NOT EXISTS `t6`(
`str` VARCHAR(32) NOT NULL COMMENT "str column",
`int64` BIGINT DEFAULT "0" COMMENT "int64 column",
`float64` DOUBLE DEFAULT "0.0" COMMENT "float64 column",
`uint64` BIGINT UNSIGNED DEFAULT "0" COMMENT "uint64 column",
`datetime_6` Datetime(6) NOT NULL COMMENT "datetime.6 column",
`json_col` TEXT NOT NULL COMMENT "json column"
);
LOAD DATA INLINE FORMAT='csv', DATA='row5,1,1.1,1,2023-05-16 16:06:18.070277,"{""key1"":""insert charactor \\''""}"' INTO TABLE t6 FIELDS TERMINATED BY ',';
LOAD DATA INLINE FORMAT='csv', DATA='row5,1,1.1,1,2023-05-16 16:06:18.070277,"{""key1"":""insert charactor \\''""}"' INTO TABLE t6;
LOAD DATA INLINE FORMAT='csv', DATA='row7,1,1.1,1,2023-05-16 16:06:18.070277,"{""key1"":""newline: \\n""}"' INTO TABLE t6 FIELDS TERMINATED BY ',';
LOAD DATA INLINE FORMAT='csv', DATA='row7,1,1.1,1,2023-05-16 16:06:18.070277,"{""key1"":""newline: \\n""}"' INTO TABLE t6;
LOAD DATA INLINE FORMAT='csv', DATA='row\\8,1,1.1,1,2023-05-16 16:06:18.070277,"{""key1"":""newline""}"' INTO TABLE t6 FIELDS TERMINATED BY ',';
LOAD DATA INLINE FORMAT='csv', DATA='row\\8,1,1.1,1,2023-05-16 16:06:18.070277,"{""key1"":""newline""}"' INTO TABLE t6;
select * from t6 order by str;
CREATE TABLE `rawlog` (
`raw_item` VARCHAR(1024) NOT NULL COMMENT 'raw log item',
`node_uuid` VARCHAR(36) NOT NULL COMMENT 'node uuid, which node gen this data.',
`node_type` VARCHAR(1024) NOT NULL COMMENT 'node type in MO, val in [DN, CN, LOG]',
`span_id` VARCHAR(16) DEFAULT '0' COMMENT 'span uniq id',
`trace_id` VARCHAR(36) NOT NULL COMMENT 'trace uniq id',
`logger_name` VARCHAR(1024) NOT NULL COMMENT 'logger name',
`timestamp` DATETIME NOT NULL COMMENT 'timestamp of action',
`level` VARCHAR(1024) NOT NULL COMMENT 'log level, enum: debug, info, warn, error, panic, fatal',
`caller` VARCHAR(1024) NOT NULL COMMENT 'where it log, like: package/file.go:123',
`message` TEXT NOT NULL COMMENT 'log message',
`extra` TEXT DEFAULT '{}' COMMENT 'log dynamic fields',
`err_code` VARCHAR(1024) DEFAULT '0' COMMENT 'error code info',
`error` TEXT NOT NULL COMMENT 'error message',
`stack` VARCHAR(2048) NOT NULL COMMENT 'stack info',
`span_name` VARCHAR(1024) NOT NULL COMMENT 'span name, for example: step name of execution plan, function name in code, ...',
`parent_span_id` VARCHAR(16) DEFAULT '0' COMMENT 'parent span uniq id',
`start_time` DATETIME NOT NULL COMMENT 'start time',
`end_time` DATETIME NOT NULL COMMENT 'end time',
`duration` BIGINT UNSIGNED DEFAULT '0' COMMENT 'exec time, unit: ns',
`resource` TEXT DEFAULT '{}' COMMENT 'static resource information',
`span_kind` VARCHAR(1024) NOT NULL COMMENT 'span kind, enum: internal, statement, remote',
`statement_id` VARCHAR(36) NOT NULL COMMENT 'statement id',
`session_id` VARCHAR(36) NOT NULL COMMENT 'session id'
);
LOAD DATA INLINE FORMAT='csv', DATA='log_info,7c4dccb4-4d3c-41f8-b482-5251dc7a41bf,ALL,7c4dccb44d3c41f8,,,2024-05-06 18:28:54.956415,info,compile/sql_executor.go:355,sql_executor exec,"{""sql"":""insert into mo_catalog.mo_account(\\n\\t\\t\\t\\taccount_id,\\n\\t\\t\\t\\taccount_name,\\n\\t\\t\\t\\tstatus,\\n\\t\\t\\t\\tcreated_time,\\n\\t\\t\\t\\tcomments,\\n                create_version) values (0,\\""sys\\"",\\""open\\"",\\""2024-05-06 10:28:54\\"",\\""system account\\"",\\""1.2.0\\"");"",""txn-id"":""018f4d73053c7667a7fbe169f33a12d1"",""duration"":0.011872,""AffectedRows"":1}",0,,,,0,0001-01-01 00:00:00.000000,0001-01-01 00:00:00.000000,0,{},internal,,' INTO TABLE rawlog;
select raw_item from rawlog;