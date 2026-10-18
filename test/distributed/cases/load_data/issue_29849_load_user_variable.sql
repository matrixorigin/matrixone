drop database if exists issue_29849;
create database issue_29849;
use issue_29849;

create table load_user_variable (
    id int primary key,
    region_id int not null,
    label varchar(32) not null,
    payload_legacy varchar(64),
    payload varchar(64),
    payload_empty varchar(64),
    payload_json json
);

-- A row variable must be listed as an input field before it can be used in SET.
load data inline format='csv', data='1,10,unknown,legacy,value,true'
into table load_user_variable
fields terminated by ','
(id, region_id, label, payload_legacy)
set payload = nullif(@missing_variable, 'NULL');

load data infile '$resources/load_data/issue_29849_user_variables.csv'
into table load_user_variable
fields terminated by ','
(id, region_id, label, payload_legacy, @user_variable, @json_value)
set payload_legacy = nullif(payload_legacy, 'NULL'),
    payload = nullif(@user_variable, 'NULL'),
    payload_empty = nullif(@user_variable, ''),
    payload_json = nullif(@json_value, 'null');

select id, region_id, label,
       payload_legacy is null as legacy_is_null, payload_legacy,
       payload is null as payload_is_null, payload,
       payload_empty is null as empty_is_null, payload_empty,
       payload_json is null as json_is_null, payload_json
from load_user_variable
order by id;

-- 行变量 SET 的 COLLATE 包装必须保留 NULLIF 赋值，不能退回列默认值。
create table load_collated_nullif (
    id int primary key,
    payload varchar(32) default 'fallback'
);
load data inline format='csv', data='1,keep\n2,sentinel'
into table load_collated_nullif fields terminated by ',' (id, @v)
set payload = nullif(@v, 'sentinel');
load data inline format='csv', data='3,keep\n4,sentinel'
into table load_collated_nullif fields terminated by ',' (id, @v)
set payload = nullif(@v, 'sentinel') collate utf8mb4_bin;
select id, payload, payload is null as is_sql_null
from load_collated_nullif order by id;

-- 无效 COLLATE 必须报错，不能成功并插入默认值。
-- @regex("Unknown collation", true)
load data inline format='csv', data='5,keep'
into table load_collated_nullif fields terminated by ',' (id, @v)
set payload = nullif(@v, 'sentinel') collate no_such_load_collation;
-- @regex("COLLATION.*not valid for CHARACTER SET", true)
load data inline format='csv', data='6,keep'
into table load_collated_nullif fields terminated by ',' (id, @v)
set payload = nullif(@v, 'sentinel') collate utf8_unicode_ci;
-- 传统列 NULLIF 的原始字段映射没有表达式 owner，不能吞掉 COLLATE。
-- @regex("not supported: COLLATE on column NULLIF", true)
load data inline format='csv', data='7,keep'
into table load_collated_nullif fields terminated by ','
set payload = nullif(payload, 'sentinel') collate utf8mb4_bin;
select count(*) as rows_after_rejected_assignments from load_collated_nullif;
-- CREATE EXTERNAL TABLE 共用 NullMap 归一化，同样不能保存丢弃了 COLLATE 的定义。
-- @regex("not supported: COLLATE on column NULLIF", true)
create external table rejected_collated_external(payload varchar(32))
infile{'filepath'='$resources/load_data/issue_29849_user_variables.csv'}
fields terminated by ','
set payload = nullif(payload, 'sentinel') collate utf8mb4_bin;
select count(*) as rejected_external_count from information_schema.tables
where table_schema = database() and table_name = 'rejected_collated_external';

drop database issue_29849;
show databases like 'issue_29849';
