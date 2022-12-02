-- env prepare statement
drop user if exists drop_user_1,drop_user_2,drop_user_3,drop_user_4,drop_user_5,drop_user_6,drop_user_7,drop_user_8,drop_user_9,drop_user_10,drop_user_11;
drop user if exists drop_user_111,drop_user_112;
drop role if exists drop_u_role_1,drop_u_role_2;
-- 1.drop user存在/不存在
create user drop_user_1 identified by '111';
drop user drop_user_1;
select user_name,authentication_string from mo_catalog.mo_user where user_name='drop_user_1';
drop user drop_user_1;

--2.drop if exists存在/不存在
drop user if exists drop_user_2;
create user drop_user_2 identified by '111' comment '';
select user_name,authentication_string from mo_catalog.mo_user where user_name='drop_user_2';
drop user if exists drop_user_2;
select user_name,authentication_string from mo_catalog.mo_user where user_name='drop_user_2';

--3.异常测试：空值，内置user，语法错误
drop user "";
drop user root;
drop user dump;
drop if not exists d;
drop user if not exists d;

--4.一次删除多个user情况，覆盖多个用户中有不存在用户，无权限删除用户
create user drop_user_3 identified by '12345678',drop_user_4 identified by '12345678',drop_user_5 identified by '12345678',drop_user_6 identified by '12345678',drop_user_7 identified by '12345678',drop_user_8 identified by '12345678',drop_user_9 identified by '12345678',drop_user_10 identified by '12345678';
select user_name,authentication_string from mo_catalog.mo_user where user_name like 'drop_user_%';
drop user drop_user_3,drop_user_4,drop_user_5;
select user_name,authentication_string from mo_catalog.mo_user where user_name like 'drop_user_%';
drop user drop_user_3,drop_user_4,drop_user_5,drop_user_6,drop_user_7;
select user_name,authentication_string from mo_catalog.mo_user where user_name like 'drop_user_%';
drop user if exists drop_user_3,drop_user_4,drop_user_5,drop_user_6,drop_user_7;
select user_name,authentication_string from mo_catalog.mo_user where user_name like 'drop_user_%';
drop user if exists drop_user_8;
select user_name,authentication_string from mo_catalog.mo_user where user_name in ('drop_user_8');
drop user drop_user_8,drop_user_user;

--5.role to user with grant option后drop user
create role drop_u_role_1,drop_u_role_2;
create user drop_user_111 identified by '111',drop_user_112 identified by '111';
grant all on table *.* to drop_u_role_1 with grant option;
grant drop_u_role_1 to drop_user_111 with grant option;
grant drop_u_role_2 to drop_user_112;
select role_name from mo_catalog.mo_role where role_name in ('drop_u_role_1','drop_u_role_2');
grant drop_u_role_1 to drop_u_role_2;
drop user drop_user_111;
select user_name,authentication_string from mo_catalog.mo_user where  user_name='drop_user_111';
select role_name from mo_catalog.mo_role where role_name ='drop_u_role_1';
select role_name from mo_catalog.mo_user_grant mug ,mo_catalog.mo_role mr where mug.role_id=mr.role_id and mr.role_name in ('drop_u_role_1','drop_u_role_2');

drop user if exists drop_user_1,drop_user_2,drop_user_3,drop_user_4,drop_user_5,drop_user_6,drop_user_7,drop_user_8,drop_user_9,drop_user_10,drop_user_11;
drop user if exists drop_user_111,drop_user_112;
drop role if exists drop_u_role_1,drop_u_role_2;
