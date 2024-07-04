-- sys restore itself
set global enable_privilege_cache = off;
-- udf
create database db1;
use db1;
select name, db from mo_catalog.mo_user_defined_function;
create function helloworld () returns int language sql as 'select id from tbl1 limit 1';
select name, db from mo_catalog.mo_user_defined_function;
create function db1.helloworld5 () returns int language sql as 'select id from tbl1 limit 1';
select name, db from mo_catalog.mo_user_defined_function;

create snapshot snapshot1 for account sys;
-- @ignore:1
show snapshots;

drop function helloworld();
select name, db from mo_catalog.mo_user_defined_function;

create snapshot snapshot2 for account sys;
-- @ignore:1
show snapshots;

drop database db1;
select name, db from mo_catalog.mo_user_defined_function;

restore account sys from snapshot snapshot1;
-- @ignore:1
show snapshots;
select name, db from mo_catalog.mo_user_defined_function;

restore account sys from snapshot snapshot2;
-- @ignore:1
show snapshots;
select name, db from mo_catalog.mo_user_defined_function;

drop snapshot snapshot1;
drop snapshot snapshot2;
-- @ignore:1
show snapshots;

drop database if exists db1;

--stage
CREATE STAGE my_ext_stage URL='s3://load/files/';
CREATE STAGE my_ext_stage URL='s3://load/files/';
CREATE STAGE if not exists my_ext_stage URL='s3://load/files/';
SELECT stage_name from mo_catalog.mo_stages;

CREATE STAGE my_ext_stage1 URL='s3://load/files/' CREDENTIALS={'AWS_KEY_ID'='1a2b3c' ,'AWS_SECRET_KEY'='4x5y6z'};
CREATE STAGE my_ext_stage2 URL='s3://load/files/' CREDENTIALS={'AWS_KEY_ID'='1a2b3c' ,'AWS_SECRET_KEY'='4x5y6z'};
SELECT stage_name, stage_status from mo_catalog.mo_stages;

CREATE STAGE my_ext_stage3 URL='s3://load/files/' CREDENTIALS={'AWS_KEY_ID'='1a2b3c' ,'AWS_SECRET_KEY'='4x5y6z'} ENABLE = TRUE;
SELECT stage_name, stage_status from mo_catalog.mo_stages;

create snapshot snapshot1 for account sys;
-- @ignore:1
show snapshots;

ALTER STAGE my_ext_stage4 SET URL='s3://load/files2/';
ALTER STAGE if exists my_ext_stage4 SET URL='s3://load/files2/';
ALTER STAGE my_ext_stage1 SET URL='s3://load/files2/' CREDENTIALS={'AWS_KEY_ID'='1a2b3d' ,'AWS_SECRET_KEY'='4x5y6z'};
ALTER STAGE my_ext_stage1 SET URL='s3://load/files2/';
ALTER STAGE my_ext_stage1 SET CREDENTIALS={'AWS_KEY_ID'='1a2b3d' ,'AWS_SECRET_KEY'='4x5y6z'};

create snapshot snapshot2 for account sys;
-- @ignore:1
show snapshots;

DROP STAGE my_ext_stage5;
DROP STAGE if exists my_ext_stage5;
DROP STAGE my_ext_stage;
DROP STAGE my_ext_stage1;
DROP STAGE my_ext_stage2;
DROP STAGE my_ext_stage3;

restore account sys from snapshot snapshot1;
-- @ignore:1
show snapshots;
SELECT stage_name from mo_catalog.mo_stages;

restore account sys from snapshot snapshot2;
-- @ignore:1
show snapshots;
SELECT stage_name from mo_catalog.mo_stages;

drop snapshot snapshot1;
drop snapshot snapshot2;
-- @ignore:1
show snapshots;

DROP STAGE if exists my_ext_stage5;
DROP STAGE if exists my_ext_stage5;
DROP STAGE if exists my_ext_stage;
DROP STAGE if exists my_ext_stage1;
DROP STAGE if exists my_ext_stage2;
DROP STAGE if exists my_ext_stage3;

show stages;

-- stored_procedure

drop database if exists procedure_test;
create database procedure_test;
use procedure_test;

create table tbh1(id int primary key, val int);
create table tbh2(id int primary key, val char);
create table tbh3(id int primary key, val float);

insert into tbh1(id, val) values(1,10),(2,20),(3,30);
insert into tbh2(id, val) values(1,'a'),(2,'b'),(3,'c');
insert into tbh3(id, val) values(1,1.5),(2,2.5),(3,3.5);

select name from mo_catalog.mo_stored_procedure;
drop procedure if exists test_if_hit_if;
-- @delimiter .
create procedure test_if_hit_if () 'begin DECLARE v1 INT; SET v1 = 10; IF v1 > 5 THEN select * from tbh1; ELSEIF v1 = 5 THEN select * from tbh2; ELSEIF v1 = 4 THEN select * from tbh2 limit 1; ELSE select * from tbh3; END IF; end'
.
-- @delimiter ;
call test_if_hit_if();
select name from mo_catalog.mo_stored_procedure;


create snapshot snapshot1 for account sys;
-- @ignore:1
show snapshots;

drop procedure if exists test_if_hit_if;
drop procedure if exists test_if_hit_elseif_first_elseif;
-- @delimiter .
create procedure test_if_hit_elseif_first_elseif() 'begin DECLARE v1 INT; SET v1 = 5; IF v1 > 5 THEN select * from tbh1; ELSEIF v1 = 5 THEN select * from tbh2; ELSEIF v1 = 4 THEN select * from tbh2 limit 1; ELSE select * from tbh3; END IF; end'
.
-- @delimiter ;
call test_if_hit_elseif_first_elseif();
select name from mo_catalog.mo_stored_procedure;

create snapshot snapshot2 for account sys;
-- @ignore:1
show snapshots;

restore account sys from snapshot snapshot1;
-- @ignore:1
show snapshots;
select name from mo_catalog.mo_stored_procedure;
call test_if_hit_if();

restore account sys from snapshot snapshot2;
-- @ignore:1
show snapshots;
select name from mo_catalog.mo_stored_procedure;
call test_if_hit_elseif_first_elseif();

drop snapshot snapshot1;
drop snapshot snapshot2;
-- @ignore:1
show snapshots;

drop procedure if exists test_if_hit_if;
drop procedure if exists test_if_hit_elseif_first_elseif;
drop database if exists procedure_test;

-- @ignore:0
select user_id,user_name,authentication_string from mo_catalog.mo_user;
-- @ignore:0,1
select role_id,user_id from mo_catalog.mo_user_grant;
-- user
create user u_a identified by 'a111', u_b identified by 'b111';
create user u_c identified by 'c111', u_d identified by 'd111';
-- @ignore:1
-- @ignore:0
select user_id,user_name,authentication_string from mo_catalog.mo_user;
-- @ignore:0,1
select role_id,user_id from mo_catalog.mo_user_grant;

create snapshot snapshot1 for account sys;
-- @ignore:1
show snapshots;
drop user if exists u_a, u_b, u_d;
alter user u_c identified by 'c111111';
-- @ignore:1
-- @ignore:0
select user_id,user_name,authentication_string from mo_catalog.mo_user;
-- @ignore:0,1
select role_id,user_id from mo_catalog.mo_user_grant;

create snapshot snapshot2 for account sys;
-- @ignore:1
show snapshots;

restore account sys from snapshot snapshot1;
-- @ignore:1
show snapshots;
-- @ignore:1
-- @ignore:0
select user_id,user_name,authentication_string from mo_catalog.mo_user;
-- @ignore:0,1
select role_id,user_id from mo_catalog.mo_user_grant;

restore account sys from snapshot snapshot2;
-- @ignore:1
show snapshots;
-- @ignore:1
-- @ignore:0
select user_id,user_name,authentication_string from mo_catalog.mo_user;
-- @ignore:0,1
select role_id,user_id from mo_catalog.mo_user_grant;

drop snapshot snapshot1;
drop snapshot snapshot2;
-- @ignore:1
show snapshots;

drop user if exists u_a, u_b, u_c, u_d;
-- @ignore:0
select user_id,user_name,authentication_string from mo_catalog.mo_user;
-- @ignore:0,1
select role_id,user_id from mo_catalog.mo_user_grant;

-- normal account restore itself
drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';

-- @session:id=1&user=acc01:test_account&password=111
-- udf
create database db1;
use db1;
select name, db from mo_catalog.mo_user_defined_function;
create function helloworld () returns int language sql as 'select id from tbl1 limit 1';
select name, db from mo_catalog.mo_user_defined_function;
create function db1.helloworld5 () returns int language sql as 'select id from tbl1 limit 1';
select name, db from mo_catalog.mo_user_defined_function;

create snapshot snapshot1 for account acc01;
-- @ignore:1
show snapshots;

drop function helloworld();
select name, db from mo_catalog.mo_user_defined_function;

create snapshot snapshot2 for account acc01;
-- @ignore:1
show snapshots;

drop database db1;
select name, db from mo_catalog.mo_user_defined_function;

restore account acc01 from snapshot snapshot1;
-- @ignore:1
show snapshots;
select name, db from mo_catalog.mo_user_defined_function;

restore account acc01 from snapshot snapshot2;
-- @ignore:1
show snapshots;
select name, db from mo_catalog.mo_user_defined_function;

drop snapshot snapshot1;
drop snapshot snapshot2;
-- @ignore:1
show snapshots;

drop database if exists db1;

--stage
CREATE STAGE my_ext_stage URL='s3://load/files/';
CREATE STAGE my_ext_stage URL='s3://load/files/';
CREATE STAGE if not exists my_ext_stage URL='s3://load/files/';
SELECT stage_name from mo_catalog.mo_stages;

CREATE STAGE my_ext_stage1 URL='s3://load/files/' CREDENTIALS={'AWS_KEY_ID'='1a2b3c' ,'AWS_SECRET_KEY'='4x5y6z'};
CREATE STAGE my_ext_stage2 URL='s3://load/files/' CREDENTIALS={'AWS_KEY_ID'='1a2b3c' ,'AWS_SECRET_KEY'='4x5y6z'};
SELECT stage_name, stage_status from mo_catalog.mo_stages;

CREATE STAGE my_ext_stage3 URL='s3://load/files/' CREDENTIALS={'AWS_KEY_ID'='1a2b3c' ,'AWS_SECRET_KEY'='4x5y6z'} ENABLE = TRUE;
SELECT stage_name, stage_status from mo_catalog.mo_stages;

create snapshot snapshot1 for account acc01;
-- @ignore:1
show snapshots;

ALTER STAGE my_ext_stage4 SET URL='s3://load/files2/';
ALTER STAGE if exists my_ext_stage4 SET URL='s3://load/files2/';
ALTER STAGE my_ext_stage1 SET URL='s3://load/files2/' CREDENTIALS={'AWS_KEY_ID'='1a2b3d' ,'AWS_SECRET_KEY'='4x5y6z'};
ALTER STAGE my_ext_stage1 SET URL='s3://load/files2/';
ALTER STAGE my_ext_stage1 SET CREDENTIALS={'AWS_KEY_ID'='1a2b3d' ,'AWS_SECRET_KEY'='4x5y6z'};

create snapshot snapshot2 for account acc01;
-- @ignore:1
show snapshots;

DROP STAGE my_ext_stage5;
DROP STAGE if exists my_ext_stage5;
DROP STAGE my_ext_stage;
DROP STAGE my_ext_stage1;
DROP STAGE my_ext_stage2;
DROP STAGE my_ext_stage3;

restore account acc01 from snapshot snapshot1;
-- @ignore:1
show snapshots;
SELECT stage_name from mo_catalog.mo_stages;

restore account acc01 from snapshot snapshot2;
-- @ignore:1
show snapshots;
SELECT stage_name from mo_catalog.mo_stages;

drop snapshot snapshot1;
drop snapshot snapshot2;
-- @ignore:1
show snapshots;

DROP STAGE if exists my_ext_stage5;
DROP STAGE if exists my_ext_stage5;
DROP STAGE if exists my_ext_stage;
DROP STAGE if exists my_ext_stage1;
DROP STAGE if exists my_ext_stage2;
DROP STAGE if exists my_ext_stage3;

show stages;

-- stored_procedure

drop database if exists procedure_test;
create database procedure_test;
use procedure_test;

create table tbh1(id int primary key, val int);
create table tbh2(id int primary key, val char);
create table tbh3(id int primary key, val float);

insert into tbh1(id, val) values(1,10),(2,20),(3,30);
insert into tbh2(id, val) values(1,'a'),(2,'b'),(3,'c');
insert into tbh3(id, val) values(1,1.5),(2,2.5),(3,3.5);

select name from mo_catalog.mo_stored_procedure;
drop procedure if exists test_if_hit_if;
-- @delimiter .
create procedure test_if_hit_if () 'begin DECLARE v1 INT; SET v1 = 10; IF v1 > 5 THEN select * from tbh1; ELSEIF v1 = 5 THEN select * from tbh2; ELSEIF v1 = 4 THEN select * from tbh2 limit 1; ELSE select * from tbh3; END IF; end'
.
-- @delimiter ;
call test_if_hit_if();
select name from mo_catalog.mo_stored_procedure;


create snapshot snapshot1 for account acc01;
-- @ignore:1
show snapshots;

drop procedure if exists test_if_hit_if;
drop procedure if exists test_if_hit_elseif_first_elseif;
-- @delimiter .
create procedure test_if_hit_elseif_first_elseif() 'begin DECLARE v1 INT; SET v1 = 5; IF v1 > 5 THEN select * from tbh1; ELSEIF v1 = 5 THEN select * from tbh2; ELSEIF v1 = 4 THEN select * from tbh2 limit 1; ELSE select * from tbh3; END IF; end'
.
-- @delimiter ;
call test_if_hit_elseif_first_elseif();
select name from mo_catalog.mo_stored_procedure;

create snapshot snapshot2 for account acc01;
-- @ignore:1
show snapshots;

restore account acc01 from snapshot snapshot1;
-- @ignore:1
show snapshots;
select name from mo_catalog.mo_stored_procedure;
call test_if_hit_if();

restore account acc01 from snapshot snapshot2;
-- @ignore:1
show snapshots;
select name from mo_catalog.mo_stored_procedure;
call test_if_hit_elseif_first_elseif();

drop snapshot snapshot1;
drop snapshot snapshot2;
-- @ignore:1
show snapshots;

drop procedure if exists test_if_hit_if;
drop procedure if exists test_if_hit_elseif_first_elseif;
drop database if exists procedure_test;

-- user
create user u_a identified by 'a111', u_b identified by 'b111' default role public lock;
create user if not exists u_a identified by 'a111', u_b identified by 'b111' default role public lock;
create user u_c identified by 'a111', u_d identified by 'b111';
-- @ignore:1
select user_name,authentication_string from mo_catalog.mo_user;

create snapshot snapshot1 for account acc01;
-- @ignore:1
show snapshots;
drop user if exists u_a, u_b, u_d;
alter user u_c identified by 'c111111';
-- @ignore:1
select user_name,authentication_string from mo_catalog.mo_user;

create snapshot snapshot2 for account acc01;
-- @ignore:1
show snapshots;

restore account acc01 from snapshot snapshot1;
-- @ignore:1
show snapshots;
-- @ignore:1
select user_name,authentication_string from mo_catalog.mo_user;

restore account acc01 from snapshot snapshot2;
-- @ignore:1
show snapshots;
-- @ignore:1
select user_name,authentication_string from mo_catalog.mo_user;

drop snapshot snapshot1;
drop snapshot snapshot2;
-- @ignore:1
show snapshots;

drop user if exists u_a, u_b, u_c, u_d;

-- @session

drop account acc01;

-- sys restore normal account to a new account
drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';

drop account if exists acc02;
create account acc02 admin_name = 'test_account' identified by '111';

drop account if exists acc03;
create account acc03 admin_name = 'test_account' identified by '111';

-- @session:id=2&user=acc01:test_account&password=111
create database db1;
use db1;
select name, db from mo_catalog.mo_user_defined_function;
create function helloworld () returns int language sql as 'select id from tbl1 limit 1';
select name, db from mo_catalog.mo_user_defined_function;
create function db1.helloworld5 () returns int language sql as 'select id from tbl1 limit 1';
select name, db from mo_catalog.mo_user_defined_function;

CREATE STAGE my_ext_stage URL='s3://load/files/';
CREATE STAGE my_ext_stage URL='s3://load/files/';
CREATE STAGE if not exists my_ext_stage URL='s3://load/files/';
SELECT stage_name from mo_catalog.mo_stages;

CREATE STAGE my_ext_stage1 URL='s3://load/files/' CREDENTIALS={'AWS_KEY_ID'='1a2b3c' ,'AWS_SECRET_KEY'='4x5y6z'};
CREATE STAGE my_ext_stage2 URL='s3://load/files/' CREDENTIALS={'AWS_KEY_ID'='1a2b3c' ,'AWS_SECRET_KEY'='4x5y6z'};
SELECT stage_name, stage_status from mo_catalog.mo_stages;

CREATE STAGE my_ext_stage3 URL='s3://load/files/' CREDENTIALS={'AWS_KEY_ID'='1a2b3c' ,'AWS_SECRET_KEY'='4x5y6z'} ENABLE = TRUE;
SELECT stage_name, stage_status from mo_catalog.mo_stages;

-- stored_procedure

drop database if exists procedure_test;
create database procedure_test;
use procedure_test;

create table tbh1(id int primary key, val int);
create table tbh2(id int primary key, val char);
create table tbh3(id int primary key, val float);

insert into tbh1(id, val) values(1,10),(2,20),(3,30);
insert into tbh2(id, val) values(1,'a'),(2,'b'),(3,'c');
insert into tbh3(id, val) values(1,1.5),(2,2.5),(3,3.5);

select name from mo_catalog.mo_stored_procedure;
drop procedure if exists test_if_hit_if;
-- @delimiter .
create procedure test_if_hit_if () 'begin DECLARE v1 INT; SET v1 = 10; IF v1 > 5 THEN select * from tbh1; ELSEIF v1 = 5 THEN select * from tbh2; ELSEIF v1 = 4 THEN select * from tbh2 limit 1; ELSE select * from tbh3; END IF; end'
.
-- @delimiter ;
call test_if_hit_if();
select name from mo_catalog.mo_stored_procedure;


-- user
create user u_a identified by 'a111', u_b identified by 'b111' default role public lock;
create user if not exists u_a identified by 'a111', u_b identified by 'b111' default role public lock;
create user u_c identified by 'a111', u_d identified by 'b111';
-- @ignore:1
select user_name,authentication_string from mo_catalog.mo_user;
-- @session

create snapshot snapshot1 for account acc01;

-- @session:id=2&user=acc01:test_account&password=111
-- udf
drop function helloworld();
select name, db from mo_catalog.mo_user_defined_function;


--stage
ALTER STAGE my_ext_stage4 SET URL='s3://load/files2/';
ALTER STAGE if exists my_ext_stage4 SET URL='s3://load/files2/';
ALTER STAGE my_ext_stage1 SET URL='s3://load/files2/' CREDENTIALS={'AWS_KEY_ID'='1a2b3d' ,'AWS_SECRET_KEY'='4x5y6z'};
ALTER STAGE my_ext_stage1 SET URL='s3://load/files2/';
ALTER STAGE my_ext_stage1 SET CREDENTIALS={'AWS_KEY_ID'='1a2b3d' ,'AWS_SECRET_KEY'='4x5y6z'};
select stage_name from mo_catalog.mo_stages;

-- stored_procedure
drop procedure if exists test_if_hit_if;
drop procedure if exists test_if_hit_if;
drop procedure if exists test_if_hit_elseif_first_elseif;
-- @delimiter .
create procedure test_if_hit_elseif_first_elseif() 'begin DECLARE v1 INT; SET v1 = 5; IF v1 > 5 THEN select * from tbh1; ELSEIF v1 = 5 THEN select * from tbh2; ELSEIF v1 = 4 THEN select * from tbh2 limit 1; ELSE select * from tbh3; END IF; end'
.
-- @delimiter ;
call test_if_hit_elseif_first_elseif();
select name from mo_catalog.mo_stored_procedure;

-- user
drop user if exists u_a, u_b, u_c, u_d;
alter user u_c identified by 'c111111';
-- @ignore:1
select user_name,authentication_string from mo_catalog.mo_user;

-- @session

create snapshot snapshot2 for account acc01;

-- @session:id=2&user=acc01:test_account&password=111
-- udf
drop database db1;
select name, db from mo_catalog.mo_user_defined_function;

--stage
DROP STAGE my_ext_stage5;
DROP STAGE if exists my_ext_stage5;
DROP STAGE my_ext_stage;
DROP STAGE my_ext_stage1;
DROP STAGE my_ext_stage2;
DROP STAGE my_ext_stage3;

-- stored_procedure
drop database if exists procedure_test;
-- @session

restore account acc01 from snapshot snapshot1 to account acc02;
-- @session:id=3&user=acc02:test_account&password=111
-- udf
select name, db from mo_catalog.mo_user_defined_function;

--stage
select stage_name from mo_catalog.mo_stages;

-- stored_procedure
select name from mo_catalog.mo_stored_procedure;

-- user
-- @ignore:1
select user_name,authentication_string from mo_catalog.mo_user;
-- @session

restore account acc01 from snapshot snapshot2 to account acc03;
-- @session:id=4&user=acc03:test_account&password=111
-- udf
select name, db from mo_catalog.mo_user_defined_function;

--stage
select stage_name from mo_catalog.mo_stages;

-- stored_procedure
select name from mo_catalog.mo_stored_procedure;

-- user
-- @ignore:1
select user_name,authentication_string from mo_catalog.mo_user;
-- @session


drop account acc01;
drop account acc02;
drop account acc03;
drop snapshot snapshot1;
drop snapshot snapshot2;
-- @ignore:1
show snapshots;

set global enable_privilege_cache = on;

-- cluster table
use mo_catalog;
drop table if exists cluster_table_1;
create cluster table cluster_table_1(a int, b int);

drop account if exists test_account1;
create account test_account1 admin_name = 'test_user' identified by '111';

drop account if exists test_account2;
create account test_account2 admin_name = 'test_user' identified by '111';

insert into cluster_table_1 values(0,0,0),(1,1,0);
insert into cluster_table_1 values(0,0,1),(1,1,1);
select * from cluster_table_1;

create snapshot snapshot1 for account sys;
drop table if exists cluster_table_1;

create cluster table cluster_table_2(a int, b int);
insert into cluster_table_2 values(0,0,0),(1,1,0);
insert into cluster_table_2 values(0,0,1),(1,1,1);
select * from cluster_table_2;

create snapshot snapshot2 for account sys;

restore account sys from snapshot snapshot1;
select * from mo_catalog.cluster_table_1;

restore account sys from snapshot snapshot2;
select * from mo_catalog.cluster_table_2;

drop snapshot snapshot1;
drop snapshot snapshot2;

drop table if exists cluster_table_1;
drop table if exists cluster_table_2;
drop database if exists procedure_test;