-- @bvt:issue#17333
set global enable_privilege_cache = off;

-- create udf, create snapshot, drop udf, restore
drop database if exists udf_db;
create database udf_db;
use udf_db;
select name, db from mo_catalog.mo_user_defined_function;
-- function add
create function `addab`(x int, y int) returns int
language sql as
'$1 + $2';
select addab(10, 5);
select name, db from mo_catalog.mo_user_defined_function;

drop snapshot if exists udf_dsp01;
create snapshot udf_dsp01 for account sys;
-- @ignore:0,1
show snapshots;

-- function concatenate
create function `concatenate`(str1 varchar(255), str2 varchar(255)) returns varchar(255)
language sql as
'$1 + $2';
select concatenate('Hello, ', 'World!');

drop snapshot if exists udf_dsp02;
create snapshot udf_dsp02 for account sys;
-- @ignore:0,1
show snapshots;

-- function sub_diff
drop database if exists udf_db2;
create database udf_db2;
use udf_db2;
create function `subab`(x int, y int) returns int
language sql as
'$1 - $2';
select subab(10, 5);
select name, db from mo_catalog.mo_user_defined_function;

drop snapshot if exists udf_dsp03;
create snapshot udf_dsp03 for account sys;
-- @ignore:0,1
show snapshots;

-- @ignore:0,9,10
select * from mo_catalog.mo_user_defined_function;

drop function subab(x int,y int);
drop function udf_db.concatenate(str1 varchar(255), str2 varchar(255));

restore account sys from snapshot udf_dsp03;
-- @ignore:0,9,10
select * from mo_catalog.mo_user_defined_function;
-- @ignore:0,1
show snapshots;

restore account sys from snapshot udf_dsp02;
-- @ignore:0,9,10
select * from mo_catalog.mo_user_defined_function;
-- @ignore:0,1
show snapshots;

drop snapshot udf_dsp01;
drop snapshot udf_dsp02;
drop snapshot udf_dsp03;
drop database udf_db;




-- create udf, drop db, restore
drop database if exists udf_db2;
create database udf_db2;
use udf_db2;
create function `addab`(x int, y int) returns int
language sql as
'$1 + $2';
-- @ignore:0,9,10
select * from mo_catalog.mo_user_defined_function;

drop snapshot if exists udf_sp04;
create snapshot udf_sp04 for account sys;

drop database udf_db2;
select * from mo_catalog.mo_user_defined_function;

restore account sys from snapshot udf_sp04;
-- @ignore:0,9,10
select * from mo_catalog.mo_user_defined_function;
drop database udf_db2;
drop snapshot udf_sp04;




-- create procedure, create snapshot, drop procedure, restore
drop database if exists procedure_test;
create database procedure_test;
use procedure_test;
drop table if exists tbh1;
drop table if exists tbh2;
drop table if exists tbh2;

create table tbh1(id int primary key, val int);
create table tbh2(id int primary key, val char);
create table tbh3(id int primary key, val float);

insert into tbh1(id, val) values(1,10),(2,20),(3,30);
insert into tbh2(id, val) values(1,'a'),(2,'b'),(3,'c');
insert into tbh3(id, val) values(1,1.5),(2,2.5),(3,3.5);

drop procedure if exists test_if_hit_elseif_first_elseif;
create procedure test_if_hit_elseif_first_elseif() 'begin DECLARE v1 INT; SET v1 = 5; IF v1 > 5 THEN select * from tbh1; ELSEIF v1 = 5 THEN select * from tbh2; ELSEIF v1 = 4 THEN select * from tbh2 limit 1; ELSE select * from tbh3; END IF; end';
call test_if_hit_elseif_first_elseif();

drop procedure if exists test_if_hit_if;
create procedure test_if_hit_if() 'begin DECLARE v1 INT; SET v1 = 5; IF v1 > 5 THEN select * from tbh1; ELSEIF v1 = 5 THEN select * from tbh2; ELSEIF v1 = 4 THEN select * from tbh2 limit 1; ELSE select * from tbh3; END IF; end';
call test_if_hit_if();
-- @ignore:0,7,8
select * from mo_catalog.mo_stored_procedure;

drop snapshot if exists sp_sp05;
create snapshot sp_sp05 for account sys;

drop procedure test_if_hit_elseif_first_elseif;
drop procedure test_if_hit_if;

restore account sys from snapshot sp_sp05;

-- @ignore:0,7,8
select * from mo_catalog.mo_stored_procedure;
call test_if_hit_elseif_first_elseif();
call test_if_hit_if();
drop snapshot sp_sp05;
drop procedure test_if_hit_elseif_first_elseif;
drop procedure test_if_hit_if;
drop database procedure_test;




-- create procedure, create snapshot, drop table, restore
drop database if exists procedure_test;
create database procedure_test;
use procedure_test;

drop table if exists tbh1;
drop table if exists tbh2;
drop table if exists tbh2;
create table tbh1(id int primary key, val int);
create table tbh2(id int primary key, val char);
create table tbh3(id int primary key, val float);

insert into tbh1(id, val) values(1,10),(2,20),(3,30);
insert into tbh2(id, val) values(1,'a'),(2,'b'),(3,'c');
insert into tbh3(id, val) values(1,1.5),(2,2.5),(3,3.5);

drop procedure if exists test_if_hit_second_elseif;
create procedure test_if_hit_second_elseif() 'begin DECLARE v1 INT; SET v1 = 4; IF v1 > 5 THEN select * from tbh1; ELSEIF v1 = 5 THEN select * from tbh2; ELSEIF v1 = 4 THEN select * from tbh2 limit 1; ELSE select * from tbh3; END IF; end';
call test_if_hit_second_elseif();

drop procedure if exists test_if_hit_else;
create procedure test_if_hit_else() 'begin DECLARE v1 INT; SET v1 = 3; IF v1 > 5 THEN select * from tbh1; ELSEIF v1 = 5 THEN select * from tbh2; ELSEIF v1 = 4 THEN select * from tbh2 limit 1; ELSE select * from tbh3; END IF; end';
call test_if_hit_else();
-- @ignore:0,7,8
select * from mo_catalog.mo_stored_procedure;

drop snapshot if exists sp_sp06;
create snapshot sp_sp06 for account sys;

drop table tbh1;
drop table tbh2;
drop procedure test_if_hit_second_elseif;
-- @ignore:0,7,8
select * from mo_catalog.mo_stored_procedure;

restore account sys from snapshot sp_sp06;

call test_if_hit_else();
call test_if_hit_second_elseif();
-- @ignore:0,7,8
select * from mo_catalog.mo_stored_procedure;

drop snapshot sp_sp06;
drop procedure test_if_hit_second_elseif;
drop procedure test_if_hit_else;
drop database procedure_test;




-- restore mo_stage
drop stage if exists my_ext_stage;
create stage my_ext_stage URL='s3://load/files/';
drop stage if exists my_ext_stage1;
create stage my_ext_stage1 URL='s3://load/files/' CREDENTIALS={'AWS_KEY_ID'='1a2b3c' ,'AWS_SECRET_KEY'='4x5y6z'};
-- @ignore:0,5
select * from mo_catalog.mo_stages;

drop snapshot if exists stage_sp01;
create snapshot stage_sp01 for account sys;

alter stage my_ext_stage1 SET URL='s3://load/files2/';
-- @ignore:0,5
select * from mo_catalog.mo_stages;
drop stage my_ext_stage;
-- @ignore:0,5
select * from mo_catalog.mo_stages;

restore account sys from snapshot stage_sp01;
-- @ignore:0,5
select * from mo_catalog.mo_stages;
drop snapshot stage_sp01;
drop stage my_ext_stage;
drop stage my_ext_stage1;




-- restore mo_user
drop user if exists userx;
create user userx identified by '111';
drop user if exists usery;
create user usery identified by '222';
select user_name, authentication_string, status, login_type, creator, owner, default_role from mo_catalog.mo_user;

drop snapshot if exists user_sp01;
create snapshot user_sp01 for account sys;

drop user if exists userz;
create user userz identified by '111';

drop snapshot if exists user_sp02;
create snapshot user_sp02 for account sys;

restore account sys from snapshot user_sp01;
select user_name, authentication_string, status, login_type, creator, owner, default_role from mo_catalog.mo_user;

restore account sys from snapshot user_sp02;
select user_name, authentication_string, status, login_type, creator, owner, default_role from mo_catalog.mo_user;
drop user userx;
drop user usery;
drop user userz;
drop snapshot user_sp01;
drop snapshot user_sp02;




-- restore mo_role
drop role if exists role1;
drop role if exists role2;
create role role1;
create role role2;
select role_name, creator, owner from mo_catalog.mo_role;
drop snapshot if exists role_sp01;
create snapshot role_sp01 for account sys;

drop role role1;
drop role role2;

restore account sys from snapshot role_sp01;

select role_name, creator, owner from mo_catalog.mo_role;
drop snapshot role_sp01;
drop role role1;
drop role role2;




-- grant privs role
drop database if exists grant_db;
create database testdb;
drop role if exists test_role;
create role test_role;

grant select,insert,update on table testdb.* to test_role with grant option;
grant all on account * to test_role;
grant ownership on database *.* to test_role;
grant ownership on table *.* to test_role;

select role_name, creator, owner from mo_catalog.mo_role;
select role_name, privilege_id, with_grant_option from mo_catalog.mo_role_privs where role_name='test_role';

drop snapshot if exists prvis_sp01;
create snapshot prvis_sp01 for account sys;

drop role test_role;
select role_name, creator, owner from mo_catalog.mo_role;
select role_name, privilege_id, with_grant_option from mo_catalog.mo_role_privs where role_name='test_role';

restore account sys from snapshot prvis_sp01;

select role_name, creator, owner from mo_catalog.mo_role;
select role_name, privilege_id, with_grant_option from mo_catalog.mo_role_privs where role_name='test_role';

drop database testdb;
drop role test_role;
drop snapshot prvis_sp01;




-- grant role to user
drop user if exists user_grant_2;
create user if not exists user_grant_2 identified by '123456';
drop role if exists role_account_priv_1;
create role 'role_account_priv_1';
grant create user, drop user, alter user, create role, drop role, create database,drop database,show databases,connect,manage grants on account *  to role_account_priv_1 with grant option;
grant select on table *.* to role_account_priv_1;
grant role_account_priv_1 to user_grant_2;

select user_name, authentication_string, status, login_type, creator, owner, default_role from mo_catalog.mo_user;
select role_name, creator, owner from mo_catalog.mo_role;
select role_name, privilege_id, with_grant_option from mo_catalog.mo_role_privs where role_name='role_account_priv_1';

drop snapshot if exists grant_sp01;
create snapshot grant_sp01 for account sys;

drop user user_grant_2;
drop role 'role_account_priv_1';

restore account sys from snapshot grant_sp01;

select user_name, authentication_string, status, login_type, creator, owner, default_role from mo_catalog.mo_user;
select role_name, creator, owner from mo_catalog.mo_role;
select role_name, privilege_id, with_grant_option from mo_catalog.mo_role_privs where role_name='role_account_priv_1';

drop user user_grant_2;
drop role role_account_priv_1;
drop snapshot grant_sp01;




-- create user, role, snapshot, grant role to user, restore
drop user if exists user_grant_3;
create user if not exists user_grant_3 identified by '123456';
drop role if exists role_account_priv_3;
create role 'role_account_priv_3';
drop snapshot if exists grant_sp02;
create snapshot grant_sp02 for account sys;

select user_name, authentication_string, status, login_type, creator, owner, default_role from mo_catalog.mo_user;
select role_name, creator, owner from mo_catalog.mo_role;
select role_name, privilege_id, with_grant_option from mo_catalog.mo_role_privs where role_name='role_account_priv_3';

grant create user, drop user, alter user, create role, drop role, create database,drop database,show databases,connect,manage grants on account *  to role_account_priv_3 with grant option;
grant select on table *.* to role_account_priv_3;
grant role_account_priv_3 to user_grant_3;

select user_name, authentication_string, status, login_type, creator, owner, default_role from mo_catalog.mo_user;
select role_name, creator, owner from mo_catalog.mo_role;
select role_name, privilege_id, with_grant_option from mo_catalog.mo_role_privs where role_name='role_account_priv_3';

restore account sys from snapshot grant_sp02;

select user_name, authentication_string, status, login_type, creator, owner, default_role from mo_catalog.mo_user;
select role_name, creator, owner from mo_catalog.mo_role;
select role_name, privilege_id, with_grant_option from mo_catalog.mo_role_privs where role_name='role_account_priv_3';

drop user user_grant_3;
drop role role_account_priv_3;
drop snapshot grant_sp02;




-- multiple permissions are granted to multiple roles
drop role if exists r1,r2,r3,r4,r5,r6,r7,r8,r9,r10;
create role r1,r2,r3,r4,r5,r6,r7,r8,r9,r10;
grant select,insert,update on table *.* to r1,r2,r3,r4,r5;

select role_name, creator, owner from mo_catalog.mo_role;
select role_name, privilege_id, with_grant_option from mo_catalog.mo_role_privs where role_name in ('r1','r2','r3','r4','r5');

drop snapshot if exists sp01;
create snapshot sp01 for account sys;

drop role r1,r2,r3,r4,r5;
select role_name, creator, owner from mo_catalog.mo_role;
select role_name, privilege_id, with_grant_option from mo_catalog.mo_role_privs where role_name in ('r1','r2','r3','r4','r5');

restore account sys from snapshot sp01;

select role_name, creator, owner from mo_catalog.mo_role;
select role_name, privilege_id, with_grant_option from mo_catalog.mo_role_privs where role_name in ('r1','r2','r3','r4','r5');

drop snapshot sp01;
drop role r1,r2,r3,r4,r5,r6,r7,r8,r9,r10;




--multi role grant to multi role
drop role if exists r1, r2, r6, r7;
create role r1, r2, r6, r7;
grant select ,insert ,update on table *.* to r1,r2 with grant option;
grant r1,r2 to r6,r7;
select mr.role_name,mp.role_name,obj_type,privilege_name,privilege_level from mo_catalog.mo_role_grant mg,mo_catalog.mo_role mr ,mo_catalog.mo_role_privs mp where  mg.grantee_id=mr.role_id and mg.granted_id = mp.role_id and mr.role_name in ('r6','r7');
select role_name, creator, owner from mo_catalog.mo_role;
select role_name, privilege_id, with_grant_option from mo_catalog.mo_role_privs where role_name in ('r1', 'r2');

drop snapshot if exists sp02;
create snapshot sp02 for account sys;

drop role r1, r2;
select role_name, creator, owner from mo_catalog.mo_role;
select role_name, privilege_id, with_grant_option from mo_catalog.mo_role_privs where role_name in ('r1', 'r2');

restore account sys from snapshot sp02;

select role_name, creator, owner from mo_catalog.mo_role;
select role_name, privilege_id, with_grant_option from mo_catalog.mo_role_privs where role_name in ('r1', 'r2');

drop snapshot sp02;
drop role r1, r2, r6, r7;




-- single role grant to multi users
drop role if exists r5;
create role r5;
drop user if exists user01, user02, user03, user04, user05;
create user user01 identified by '123456';
create user user02 identified by '123456';
create user user03 identified by '123456';
create user user04 identified by '123456';
create user user05 identified by '123456';
grant create role on account * to r5;
grant r5 to user01, user02, user03, user04, user05;
select user_name,role_name,obj_type,privilege_name,privilege_level from mo_catalog.mo_user_grant,mo_catalog.mo_user,mo_catalog.mo_role_privs where mo_user_grant.user_id=mo_user.user_id and mo_role_privs.role_id=mo_user_grant.role_id and role_name in ('r5');
select user_name, authentication_string, status, login_type, creator, owner, default_role from mo_catalog.mo_user;
select role_name, creator, owner from mo_catalog.mo_role;
select role_name, privilege_id, with_grant_option from mo_catalog.mo_role_privs where role_name in ('r5');

drop snapshot if exists sp03;
create snapshot sp03 for account sys;
drop role r5;
drop user user01, user02, user03;

select user_name,role_name,obj_type,privilege_name,privilege_level from mo_catalog.mo_user_grant,mo_catalog.mo_user,mo_catalog.mo_role_privs where mo_user_grant.user_id=mo_user.user_id and mo_role_privs.role_id=mo_user_grant.role_id and role_name in ('r5');
select user_name, authentication_string, status, login_type, creator, owner, default_role from mo_catalog.mo_user;
select role_name, creator, owner from mo_catalog.mo_role;
select role_name, privilege_id, with_grant_option from mo_catalog.mo_role_privs where role_name in ('r5');

restore account sys from snapshot sp03;

select user_name, authentication_string, status, login_type, creator, owner, default_role from mo_catalog.mo_user;
select role_name, creator, owner from mo_catalog.mo_role;
select role_name, privilege_id, with_grant_option from mo_catalog.mo_role_privs where role_name in ('r5');

drop user user01, user02, user03, user04, user05;
drop role r5;
drop snapshot sp03;




-- grant, create snapshot, revoke, restore
drop role if exists role_r1,role_r2,role_r3;
create role role_r1,role_r2,role_r3;
drop user if exists role_u1, role_u2, role_u3;
create user role_u1 identified by '111', role_u2 identified by '111', role_u3 identified by '111';
grant role_r1 to role_u1;
grant role_r1,role_r2,role_r3 to role_u1,role_u2,role_u2;
grant role_r1 to role_r2;
grant role_r2 to role_r3;
select user_name, authentication_string, status, login_type, creator, owner, default_role from mo_catalog.mo_user;
select role_name, creator, owner from mo_catalog.mo_role;
select role_name, privilege_id, with_grant_option from mo_catalog.mo_role_privs where role_name in ('role_r1','role_r2');
select operation_role_id,operation_user_id from mo_catalog.mo_role_grant;

drop snapshot if exists sp01;
create snapshot sp01 for account sys;

revoke role_r2 from role_r3;
revoke role_r1 from role_r2;

select operation_role_id,operation_user_id from mo_catalog.mo_role_grant;
restore account sys from snapshot sp01;
select operation_role_id,operation_user_id from mo_catalog.mo_role_grant;
drop snapshot sp01;
drop role role_r1, role_r2, role_r3;
drop user role_u1, role_u2, role_u3;
set global enable_privilege_cache = on;
-- @bvt:issue
