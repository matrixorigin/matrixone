-- #31232: LOAD DATA INLINE must apply the same parsed source options as the
-- file path: SET NULLIF mappings (csv and jsonline), LINES STARTING BY
-- prefixes, IGNORE LINES, and their combination with byte-identical INFILE
-- controls.

drop table if exists t31232_inline_set;
create table t31232_inline_set(id int primary key, s varchar(20), flag int);
load data inline format='csv', data='1,alpha,0\n2,beta,7\n' into table t31232_inline_set fields terminated by ',' set flag=nullif(flag,0);
select * from t31232_inline_set order by id;
drop table t31232_inline_set;

drop table if exists t31232_file_set;
create table t31232_file_set(id int primary key, s varchar(20), flag int);
load data infile '$resources/load_data/issue_31232_inline_set.csv' into table t31232_file_set fields terminated by ',' set flag=nullif(flag,0);
select * from t31232_file_set order by id;
drop table t31232_file_set;

-- control: inline without SET keeps the raw values
drop table if exists t31232_inline_ctrl;
create table t31232_inline_ctrl(id int primary key, s varchar(20), flag int);
load data inline format='csv', data='1,alpha,0\n2,beta,7\n' into table t31232_inline_ctrl fields terminated by ',';
select * from t31232_inline_ctrl order by id;
drop table t31232_inline_ctrl;

-- both NULLIF sentinels of one column must survive inline transport
drop table if exists t31232_inline_multi;
create table t31232_inline_multi(id int primary key, flag int);
load data inline format='csv', data='1,0\n2,7\n3,5\n' into table t31232_inline_multi fields terminated by ',' set flag=nullif(flag,0), flag=nullif(flag,7);
select * from t31232_inline_multi order by id;
drop table t31232_inline_multi;

drop table if exists t31232_inline_json;
create table t31232_inline_json(id int primary key, flag int);
load data inline format='jsonline', data='{"id":1,"flag":0}\n{"id":2,"flag":7}\n', jsontype='object' into table t31232_inline_json set flag=nullif(flag,0);
select * from t31232_inline_json order by id;
drop table t31232_inline_json;

drop table if exists t31232_file_json;
create table t31232_file_json(id int, flag int);
load data infile {'filepath'='$resources/load_data/issue_31232_inline_set.jl','format'='jsonline','jsondata'='object'} into table t31232_file_json set flag=nullif(flag,0);
select * from t31232_file_json order by id;
drop table t31232_file_json;

-- LINES STARTING BY and SET NULLIF combine inline
drop table if exists t31232_inline_prefix;
create table t31232_inline_prefix(id int primary key, s varchar(20), flag int);
load data inline format='csv', data='DATA:1,alpha,0\nDATA:2,beta,7\n' into table t31232_inline_prefix fields terminated by ',' lines starting by 'DATA:' set flag=nullif(flag,0);
select * from t31232_inline_prefix order by id;
drop table t31232_inline_prefix;

drop table if exists t31232_file_prefix;
create table t31232_file_prefix(id int primary key, s varchar(20), flag int);
load data infile '$resources/load_data/issue_31232_inline_prefix.csv' into table t31232_file_prefix fields terminated by ',' lines starting by 'DATA:' set flag=nullif(flag,0);
select * from t31232_file_prefix order by id;
drop table t31232_file_prefix;

-- IGNORE LINES also transfers through the inline rebuild
drop table if exists t31232_inline_ignore;
create table t31232_inline_ignore(id int primary key, flag int);
load data inline format='csv', data='junk,ignore\n1,0\n2,7\n' into table t31232_inline_ignore fields terminated by ',' ignore 1 lines set flag=nullif(flag,0);
select * from t31232_inline_ignore order by id;
drop table t31232_inline_ignore;

-- unsupported SET expressions keep being rejected before any row is inserted
drop table if exists t31232_inline_reject;
create table t31232_inline_reject(id int primary key, flag int);
load data inline format='csv', data='1,0\n2,7\n' into table t31232_inline_reject fields terminated by ',' set flag=flag+1;
select count(*) from t31232_inline_reject;
drop table t31232_inline_reject;
