CREATE STAGE my_ext_stage URL='s3://load/files/';
CREATE STAGE my_ext_stage URL='s3://load/files/';
CREATE STAGE if not exists my_ext_stage URL='s3://load/files/';
SELECT stage_name from mo_catalog.mo_stages;

CREATE STAGE my_ext_stage1 URL='s3://load/files/' CREDENTIALS={'AWS_KEY_ID'='1a2b3c' ,'AWS_SECRET_KEY'='4x5y6z'};
CREATE STAGE my_ext_stage2 URL='s3://load/files/' CREDENTIALS={'AWS_KEY_ID'='1a2b3c' ,'AWS_SECRET_KEY'='4x5y6z'};
SELECT stage_name, stage_status from mo_catalog.mo_stages;

CREATE STAGE my_ext_stage3 URL='s3://load/files/' CREDENTIALS={'AWS_KEY_ID'='1a2b3c' ,'AWS_SECRET_KEY'='4x5y6z'} ENABLE = TRUE;
SELECT stage_name, stage_status from mo_catalog.mo_stages;

ALTER STAGE my_ext_stage4 SET URL='s3://load/files2/';
ALTER STAGE if exists my_ext_stage4 SET URL='s3://load/files2/';
ALTER STAGE my_ext_stage1 SET URL='s3://load/files2/' CREDENTIALS={'AWS_KEY_ID'='1a2b3d' ,'AWS_SECRET_KEY'='4x5y6z'};
ALTER STAGE my_ext_stage1 SET URL='s3://load/files2/';
ALTER STAGE my_ext_stage1 SET CREDENTIALS={'AWS_KEY_ID'='1a2b3d' ,'AWS_SECRET_KEY'='4x5y6z'};

DROP STAGE my_ext_stage5;
DROP STAGE if exists my_ext_stage5;
DROP STAGE my_ext_stage;
DROP STAGE my_ext_stage1;
DROP STAGE my_ext_stage2;
DROP STAGE my_ext_stage3;


CREATE STAGE my_ext_stage URL='s3://load/files/';
SELECT stage_name from mo_catalog.mo_stages;
create account default_1 ADMIN_NAME admin IDENTIFIED BY '111111';
-- @session:id=1&user=default_1:admin&password=111111
CREATE STAGE my_ext_stage1 URL='s3://load/files/' CREDENTIALS={'AWS_KEY_ID'='1a2b3c' ,'AWS_SECRET_KEY'='4x5y6z'};
CREATE STAGE my_ext_stage2 URL='s3://load/files/' CREDENTIALS={'AWS_KEY_ID'='1a2b3c' ,'AWS_SECRET_KEY'='4x5y6z'};
SELECT stage_name, stage_status from mo_catalog.mo_stages;
DROP STAGE my_ext_stage1;
DROP STAGE my_ext_stage2;
-- @session
drop account default_1;
drop stage my_ext_stage;


CREATE STAGE my_ext_stage URL='s3://load/files/';
CREATE STAGE my_ext_stage URL='s3://load/files/';
CREATE STAGE if not exists my_ext_stage URL='s3://load/files/';
SHOW STAGES;

CREATE STAGE my_ext_stage1 URL='s3://load/files/' CREDENTIALS={'AWS_KEY_ID'='1a2b3c' ,'AWS_SECRET_KEY'='4x5y6z'};
CREATE STAGE my_ext_stage2 URL='s3://load/files/' CREDENTIALS={'AWS_KEY_ID'='1a2b3c' ,'AWS_SECRET_KEY'='4x5y6z'};
SHOW STAGES;

CREATE STAGE my_ext_stage3 URL='s3://load/files/' CREDENTIALS={'AWS_KEY_ID'='1a2b3c' ,'AWS_SECRET_KEY'='4x5y6z'} ENABLE = TRUE;
CREATE STAGE my_ext_stage4 URL='s3://load/files/' CREDENTIALS={'AWS_KEY_ID'='1a2b3c' ,'AWS_SECRET_KEY'='4x5y6z'} ENABLE = TRUE COMMENT = 'self stage';
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
create stage aws_stage URL= 's3.us-east-2.amazonaws.com' CREDENTIALS={ 'access_key_id' = 'AKIAYOFAMAB', 'secret_access_key' = '7OtGNgIwlkBVwyL9rV', 'bucket' = 'hn-test2', 'region' = 'us-east-2', 'compression' = 'none'};
show stages;
alter stage aws_stage set enable=TRUE;
show stages;
alter stage if exists aws_stage set URL= 's3.us-east-1.amazonaws.com';
show stages;
alter stage if exists aws_stage set CREDENTIALS={ 'bucket' = 'hn-test2', 'region' = 'us-east-1'};
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
create stage local_stage URL= '$resources/into_outfile' ENABLE=TRUE comment='local stage configure';
select stage_name,stage_status,comment from mo_catalog.mo_stages;
select * from stage_table into outfile 'local_stage:/stage_table.csv';
drop stage local_stage;
show stages;

-- url params error
-- create stage local_stage URL= '$resources/abc' ENABLE=TRUE;
-- select * from stage_table into outfile 'local_stage:/stage_table.csv';
-- drop stage local_stage;

-- enable is false
create stage local_stage URL= '$resources/into_outfile' ENABLE=FALSE;
select * from stage_table into outfile 'local_stage:/stage_table.csv';

-- select outfile without stage
select * from stage_table into outfile '$resources/into_outfile/stage_table00.csv';
alter stage local_stage set ENABLE=TRUE;
select * from stage_table into outfile '$resources/into_outfile/stage_table01.csv';
drop stage local_stage;
select * from stage_table into outfile '$resources/into_outfile/stage_table02.csv';

-- alter stage params: enable/URL/comment
create stage local_stage URL= '$resources/into_outfile' ENABLE=FALSE;
alter stage local_stage set ENABLE=TRUE;
select stage_name,stage_status,comment from mo_catalog.mo_stages;
select * from stage_table into outfile 'local_stage:/stage_table1.csv';
truncate table stage_table;
load data infile '$resources/into_outfile/stage_table1.csv' into table stage_table fields terminated by ',' ignore 1 lines;
select r_name from stage_table;
alter stage local_stage set URL= '$resources/into_outfile_2';
select stage_name,stage_status,comment from mo_catalog.mo_stages;
select * from stage_table into outfile 'local_stage:/stage_table2.csv';
truncate table stage_table;
load data infile '$resources/into_outfile_2/stage_table2.csv' into table stage_table fields terminated by ',' ignore 1 lines;
select r_name from stage_table;
alter stage local_stage set comment = 'new comment';
select stage_name,stage_status,comment from mo_catalog.mo_stages;

-- abnormal test
create stage local_stage URL= '$resources/into_outfile_2' ENABLE=FALSE;
alter stage local_stage set URL= '$resources/into_outfile_2' comment='alter comment';
drop stage local_stage;
--create stage local_stage URL= '$resources/into_outfile_2' ENABLE=ffalse;

-- select outfile file exists
drop stage if exists local_stage;
create stage local_stage URL= '$resources/into_outfile' ENABLE=TRUE comment='local stage configure';
select * from stage_table into outfile 'local_stage:/stage_table3.csv';
--select * from stage_table into outfile 'local_stage:/stage_table3.csv';
drop stage local_stage;

-- if exists
create stage if not exists local_stage URL= '$resources/into_outfile' ENABLE=TRUE comment='local stage configure';
create stage if not exists local_stage URL= '$resources/into_outfile2' ENABLE=FALSE;
select stage_name,stage_status,comment from mo_catalog.mo_stages;
alter stage if exists l_stage set ENABLE=TRUE;

-- privs confirm
create user "stage_user" identified by '123456';
create role s_role;
grant all on table *.* to s_role;
grant all on account * to s_role;
grant all on database *.* to s_role;
grant s_role to stage_user;
-- @session:id=2&user=sys:stage_user:s_role&password=123456
create stage local_stage URL= '$resources/into_outfile' ENABLE=TRUE comment='local stage configure';
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
alter stage local_stage set ENABLE=FALSE;
drop stage stage_user;
drop database sdb;
-- @session
drop user stage_user;
drop role s_role;

drop account if exists stage_account;
create account `stage_account` ADMIN_NAME 'admin' IDENTIFIED BY '123456';
-- @session:id=3&user=stage_account:admin&password=123456
create stage local_stage URL= '$resources/into_outfile' ENABLE=TRUE comment='local stage configure';
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
select * from stage_table into outfile 'local_stage:/stage_table5.csv';
drop stage local_stage;
drop database sdb;
create user "stage_user02" identified by '123456';
create role s_role;
grant all on table *.* to s_role;
grant all on account * to s_role;
grant all on database *.* to s_role;
grant s_role to stage_user02;
-- @session

-- @session:id=4&user=stage_account:stage_user02:s_role&password=123456
create stage local_stage URL= '$resources/into_outfile' ENABLE=TRUE;
show stages;
alter stage local_stage set ENABLE=FALSE;
drop stage local_stage;
-- @session
drop account stage_account;
drop database if exists stage;
drop stage if exists aws_stage;
drop stage if exists local_stage;
drop user if exists stage_user;
drop role if exists s_role;
