CREATE STAGE invalid_path_stage URL='/path/to';
CREATE STAGE invalid_unknown_protocol_stage URL='minio:///path/to';

CREATE STAGE my_ext_stage URL='s3://bucket/files/';
CREATE STAGE my_sub_stage URL='stage://my_ext_stage/a/b/c/';

CREATE STAGE my_ext_stage URL='s3://bucket/files/';
ALTER STAGE my_ext_stage SET URL='abc';


CREATE STAGE if not exists my_ext_stage URL='s3://bucket/files/';
SELECT stage_name, url from mo_catalog.mo_stages;

CREATE STAGE my_ext_stage1 URL='s3://bucket1/files/' CREDENTIALS={'AWS_KEY_ID'='1a2b3c' ,'AWS_SECRET_KEY'='4x5y6z', 'AWS_REGION'='us-east-2', 'PROVIDER'='minio'};
CREATE STAGE my_ext_stage2 URL='s3://bucket2/files/' CREDENTIALS={'AWS_KEY_ID'='1a2b3c' ,'AWS_SECRET_KEY'='4x5y6z', 'AWS_REGION'='us-east-2', 'PROVIDER'='minio'};
SELECT stage_name, url, stage_credentials from mo_catalog.mo_stages;

CREATE STAGE my_ext_stage3 URL='s3://bucket3/files/' CREDENTIALS={'AWS_KEY_ID'='1a2b3c' ,'AWS_SECRET_KEY'='4x5y6z', 'AWS_REGION'='us-east-2', 'PROVIDER'='minio'};
SELECT stage_name, url, stage_credentials from mo_catalog.mo_stages;

ALTER STAGE my_ext_stage4 SET URL='s3://bucket4/files2/';
ALTER STAGE if exists my_ext_stage4 SET URL='s3://bucket4/files2/';
ALTER STAGE my_ext_stage1 SET URL='s3://bucket2/files2/' CREDENTIALS={'AWS_KEY_ID'='1a2b3d' ,'AWS_SECRET_KEY'='4x5y6z'};
ALTER STAGE my_ext_stage1 SET URL='s3://bucket2/files2/';
ALTER STAGE my_ext_stage1 SET CREDENTIALS={'AWS_KEY_ID'='1a2b3d' ,'AWS_SECRET_KEY'='4x5y6z'};

DROP STAGE my_ext_stage5;
DROP STAGE if exists my_ext_stage5;
DROP STAGE my_ext_stage;
DROP STAGE my_ext_stage1;
DROP STAGE my_ext_stage2;
DROP STAGE my_ext_stage3;
DROP STAGE my_sub_stage;


CREATE STAGE my_ext_stage URL='s3://bucket/files/';
SELECT stage_name, url, stage_credentials from mo_catalog.mo_stages;
create account default_1 ADMIN_NAME admin IDENTIFIED BY '111111';
-- @session:id=1&user=default_1:admin&password=111111
CREATE STAGE my_ext_stage1 URL='s3://bucket1/files/' CREDENTIALS={'AWS_KEY_ID'='1a2b3c' ,'AWS_SECRET_KEY'='4x5y6z', 'AWS_REGION'='us-east-2', 'PROVIDER'='minio'};
CREATE STAGE my_ext_stage2 URL='s3://bucket2/files/' CREDENTIALS={'AWS_KEY_ID'='1a2b3c' ,'AWS_SECRET_KEY'='4x5y6z', 'AWS_REGION'='us-east-2', 'PROVIDER'='minio'};
SELECT stage_name, url, stage_credentials from mo_catalog.mo_stages;
DROP STAGE my_ext_stage1;
DROP STAGE my_ext_stage2;
-- @session
drop account default_1;
drop stage my_ext_stage;


CREATE STAGE my_ext_stage URL='s3://bucket/files/';
CREATE STAGE my_ext_stage URL='s3://bucket/files/';
CREATE STAGE if not exists my_ext_stage URL='s3://bucket/files/';
SHOW STAGES;

CREATE STAGE my_ext_stage1 URL='s3://bucket/files/' CREDENTIALS={'AWS_KEY_ID'='1a2b3c' ,'AWS_SECRET_KEY'='4x5y6z'};
CREATE STAGE my_ext_stage2 URL='s3://bucket/files/' CREDENTIALS={'AWS_KEY_ID'='1a2b3c' ,'AWS_SECRET_KEY'='4x5y6z'};
SHOW STAGES;

CREATE STAGE my_ext_stage3 URL='s3://bucket/files/' CREDENTIALS={'AWS_KEY_ID'='1a2b3c' ,'AWS_SECRET_KEY'='4x5y6z'};
CREATE STAGE my_ext_stage4 URL='s3://bucket/files/' CREDENTIALS={'AWS_KEY_ID'='1a2b3c' ,'AWS_SECRET_KEY'='4x5y6z'} COMMENT = 'self stage';
SHOW STAGES;
SHOW STAGES like 'my_ext_stage3';

ALTER STAGE my_ext_stage5 SET URL='s3://load/files2/';
ALTER STAGE if exists my_ext_stage5 SET URL='s3://load/files2/';
ALTER STAGE my_ext_stage1 SET URL='s3://load/files2/' CREDENTIALS={'AWS_KEY_ID'='1a2b3d' ,'AWS_SECRET_KEY'='4x5y6z'};
ALTER STAGE my_ext_stage1 SET URL='s3://load/files2/';
ALTER STAGE my_ext_stage1 SET CREDENTIALS={'AWS_KEY_ID'='1a2b3d' ,'AWS_SECRET_KEY'='4x5y6z'};
ALTER STAGE my_ext_stage4 SET COMMENT = 'user stage';
SHOW STAGES;
SHOW STAGES like 'my_ext_stage1';

DROP STAGE my_ext_stage5;
DROP STAGE if exists my_ext_stage5;
DROP STAGE my_ext_stage;
DROP STAGE my_ext_stage1;
DROP STAGE my_ext_stage2;
DROP STAGE my_ext_stage3;
DROP STAGE my_ext_stage4;

-- create stage aws,cos,aliyun ,now not support select outfile s3
drop stage if exists aws_stage;
drop stage if exists local_stage;
create stage aws_stage URL= 's3://hn-test2/a/b/c' CREDENTIALS={ 'AWS_KEY_ID' = 'AKIAYOFAMAB', 'AWS_SECRET_KEY' = '7OtGNgIwlkBVwyL9rV', 'AWS_REGION' = 'us-east-2', 'provider'='minio', 'compression' = 'none'};
show stages;
alter stage aws_stage set enable=TRUE;
show stages;
alter stage if exists aws_stage set URL= 's3://bucket2/d/e/f/';
show stages;
alter stage if exists aws_stage set CREDENTIALS={ 'AWS_REGION' = 'us-east-1'};
select stage_name,url,stage_credentials,stage_status,comment from mo_catalog.mo_stages;
alter stage aws_stage set comment='comment1';
show stages;
drop stage aws_stage;

-- prepare table
CREATE TABLE stage_table(
R_REGIONKEY  INTEGER NOT NULL,
R_NAME       CHAR(25) NOT NULL,
R_COMMENT    VARCHAR(152),
PRIMARY KEY (R_REGIONKEY)
);
insert into stage_table values
(0,"AFRICA","lar deposits. blithely final packages cajole. regular waters are final requests. regular accounts are according to "),
(1,"AMERICA","hs use ironic, even requests. s"),
(2,"ASIA","ges. thinly even pinto beans ca"),
(3,"EUROPE","ly final courts cajole furiously final excuse"),
(4,"MIDDLE EAST","uickly special accounts cajole carefully blithely close requests. carefully final asymptotes haggle furiousl");
-- create stage local disk
create stage local_stage URL= 'file:///$resources/into_outfile' comment='local stage configure';
select stage_name,stage_status,comment from mo_catalog.mo_stages;
select * from stage_table into outfile 'stage://local_stage/stage_table.csv';
drop stage local_stage;
show stages;

-- url params error
-- create stage local_stage URL= '$resources/abc' ENABLE=TRUE;
-- select * from stage_table into outfile 'stage://local_stage/stage_table.csv';
-- drop stage local_stage;

-- output to sub-stage file
create stage local_stage URL= 'file:///$resources/';
create stage sub_local_stage URL= 'stage://local_stage/into_outfile/';
select * from stage_table into outfile 'stage://sub_local_stage/stage_table.csv';
drop stage local_stage;
drop stage sub_local_stage;

-- select outfile without stage
select * from stage_table into outfile '$resources/into_outfile/stage_table00.csv';
select * from stage_table into outfile '$resources/into_outfile/stage_table01.csv';
select * from stage_table into outfile '$resources/into_outfile/stage_table02.csv';

-- alter stage params: enable/URL/comment
create stage local_stage URL= 'file://$resources/into_outfile';
select stage_name, url, comment from mo_catalog.mo_stages;
select * from stage_table into outfile 'stage://local_stage/stage_table1.csv';
truncate table stage_table;
load data infile '$resources/into_outfile/stage_table1.csv' into table stage_table fields terminated by ',' ignore 1 lines;
select r_name from stage_table;
alter stage local_stage set URL= 'file:///$resources/into_outfile_2';
select stage_name,stage_status,comment from mo_catalog.mo_stages;
select * from stage_table into outfile 'stage://local_stage/stage_table2.csv';
truncate table stage_table;
load data infile '$resources/into_outfile_2/stage_table2.csv' into table stage_table fields terminated by ',' ignore 1 lines;
select r_name from stage_table;
alter stage local_stage set comment = 'new comment';
select stage_name,stage_status,comment from mo_catalog.mo_stages;

-- select outfile file exists
drop stage if exists local_stage;
create stage local_stage URL= 'file:///$resources/into_outfile' comment='local stage configure';
select * from stage_table into outfile 'stage://local_stage/stage_table3.csv';
--select * from stage_table into outfile 'local_stage:/stage_table3.csv';
drop stage local_stage;

-- if exists
create stage if not exists local_stage URL= 'file:///$resources/into_outfile' comment='local stage configure';
create stage if not exists local_stage URL= 'file:///$resources/into_outfile2';
select stage_name,stage_status,comment from mo_catalog.mo_stages;

-- privs confirm
create user "stage_user" identified by '123456';
create role s_role;
grant all on table *.* to s_role;
grant all on account * to s_role;
grant all on database *.* to s_role;
grant s_role to stage_user;
-- @session:id=2&user=sys:stage_user:s_role&password=123456
create stage local_stage URL= 'file:///$resources/into_outfile' comment='local stage configure';
create database sdb;
use sdb;
CREATE TABLE stage_table(
R_REGIONKEY  INTEGER NOT NULL,
R_NAME       CHAR(25) NOT NULL,
R_COMMENT    VARCHAR(152),
PRIMARY KEY (R_REGIONKEY)
);
insert into stage_table values
(0,"AFRICA","lar deposits. blithely final packages cajole. regular waters are final requests. regular accounts are according to "),
(1,"AMERICA","hs use ironic, even requests. s"),
(2,"ASIA","ges. thinly even pinto beans ca"),
(3,"EUROPE","ly final courts cajole furiously final excuse"),
(4,"MIDDLE EAST","uickly special accounts cajole carefully blithely close requests. carefully final asymptotes haggle furiousl");
drop stage stage_user;
drop database sdb;
-- @session
drop user stage_user;
drop role s_role;

drop account if exists stage_account;
create account `stage_account` ADMIN_NAME 'admin' IDENTIFIED BY '123456';
-- @session:id=3&user=stage_account:admin&password=123456
create stage local_stage URL= 'file:///$resources/' comment='local stage configure';
create stage sub_local_stage URL= 'stage://local_stage/into_outfile' comment='sub local stage configure';
create database sdb;
use sdb;
CREATE TABLE stage_table(
R_REGIONKEY  INTEGER NOT NULL,
R_NAME       CHAR(25) NOT NULL,
R_COMMENT    VARCHAR(152),
PRIMARY KEY (R_REGIONKEY)
);
insert into stage_table values
(0,"AFRICA","lar deposits. blithely final packages cajole. regular waters are final requests. regular accounts are according to "),
(1,"AMERICA","hs use ironic, even requests. s"),
(2,"ASIA","ges. thinly even pinto beans ca"),
(3,"EUROPE","ly final courts cajole furiously final excuse"),
(4,"MIDDLE EAST","uickly special accounts cajole carefully blithely close requests. carefully final asymptotes haggle furiousl");
select stage_name,stage_status,comment from mo_catalog.mo_stages;

-- write to sub stage
select * from stage_table into outfile 'stage://sub_local_stage/stage_table5.csv';

-- read from stage file
CREATE TABLE stage_infile_table(
R_REGIONKEY  INTEGER NOT NULL,
R_NAME       CHAR(25) NOT NULL,
R_COMMENT    VARCHAR(152),
PRIMARY KEY (R_REGIONKEY)
);
load data infile 'stage://sub_local_stage/stage_table5.csv' into table stage_infile_table fields terminated by ',' IGNORE 1 LINES;
select * from stage_infile_table;

-- read from external table and sub stage
CREATE EXTERNAL TABLE stage_ext_table(
R_REGIONKEY  INTEGER NOT NULL,
R_NAME       CHAR(25) NOT NULL,
R_COMMENT    VARCHAR(152)
) INFILE 'stage://sub_local_stage/stage_table*.csv' fields terminated by ',' IGNORE 1 LINES;
select count(*) from stage_ext_table;

-- list the stage directory
select count(stage_list('stage://sub_local_stage/'));

-- list the stage directory with wildcard
select count(stage_list('stage://sub_local_stage/stage_table*.csv'));

drop stage local_stage;
drop stage sub_local_stage;
drop database sdb;
create user "stage_user02" identified by '123456';
create role s_role;
grant all on table *.* to s_role;
grant all on account * to s_role;
grant all on database *.* to s_role;
grant s_role to stage_user02;
-- @session

-- @session:id=4&user=stage_account:stage_user02:s_role&password=123456
create stage local_stage URL= 'file:///$resources/into_outfile';
show stages;
drop stage local_stage;
-- @session
drop account stage_account;
drop database if exists stage;
drop stage if exists aws_stage;
drop stage if exists local_stage;
drop user if exists stage_user;
drop role if exists s_role;
