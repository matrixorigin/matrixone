-- env prepare statement
drop role if exists drop_role1,drop_role2,drop_role3,drop_role4,drop_role5,drop_role6,drop_role7,drop_role8,drop_role9,drop_role10;
drop role if exists drop_role_001,drop_role_002,drop_role_1,drop_role_2;
drop user if exists drop_user_1,drop_user_2;

-- 1.drop role存在/不存在
create role drop_role_001;
drop role drop_role_001;
select role_name from mo_catalog.mo_role where role_name='drop_role_001';
drop role role_name;

--2.drop if exists存在/不存在
drop role if exists drop_role_002;
create role drop_role_002;
drop role if exists drop_role_002;
select role_name from mo_catalog.mo_role where role_name='drop_role_002';

--3.异常测试：空值，内置role，语法错误
drop role '';
-- @bvt:issue#5705
drop role moadmin;
drop role public;
-- @bvt:issue
drop if not exists d;
drop role if not exists d;

--4.一次删除多个role情况，覆盖多个role中有不存在role，无权限删除role
create role if not exists drop_role1,drop_role2,drop_role3,drop_role4,drop_role5,drop_role6,drop_role7,drop_role8,drop_role9,drop_role10;
drop role drop_role1,drop_role2,drop_role3,drop_role11;
drop role if exists drop_role1,drop_role2,drop_role3,drop_role11;
drop role if exists drop_role4,drop_role5,drop_role6,root,drop_role8;
drop role if exists drop_role9,drop_role10;
select role_name from mo_catalog.mo_role where role_name like 'drop_role%';

--5.role with grant option 后drop role
create role drop_role_1,drop_role_2;
create user drop_user_1 identified by '111',drop_user_2 identified by '111';
grant all on table *.* to drop_role_1 with grant option;
grant drop_role_1 to drop_user_1;
grant drop_role_2 to drop_user_2;
select role_name from mo_catalog.mo_role where role_name in ('drop_role_1','drop_role_2');
grant drop_role_1 to drop_role_2;
drop role drop_role_1;
select role_name from mo_catalog.mo_role where role_name in ('drop_role_1','drop_role_2');
select role_name from mo_catalog.mo_user_grant mug ,mo_catalog.mo_role mr where mug.role_id=mr.role_id and mr.role_name in ('drop_role_1','drop_role_2');

drop role if exists drop_role1,drop_role2,drop_role3,drop_role4,drop_role5,drop_role6,drop_role7,drop_role8,drop_role9,drop_role10;
drop role if exists drop_role_001,drop_role_002,drop_role_1,drop_role_2;
drop user if exists drop_user_1,drop_user_2;