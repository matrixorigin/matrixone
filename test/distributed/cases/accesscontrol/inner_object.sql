-- env prepare statement
drop account if exists account1;
drop account if exists inner_account;
drop role if exists revoke_role_1;

--验证访问控制表中内置对象数据正确性
select user_name,owner from mo_catalog.mo_user where user_name="root";
select role_id,role_name,owner from mo_catalog.mo_role where role_name in ("moadmin","public");

--验证moadminaccount初始化，sys租户root下创建普通租户下管理员用户查看
create account account1 ADMIN_NAME 'admin' IDENTIFIED BY '123456';
-- @session:id=2&user=account1:admin&password=123456
select role_id,role_name,owner from mo_catalog.mo_role;
-- @session

--public只有连接权限
-- @bvt:issue#6041
-- @session:id=2&user=account1:admin:public&password=123456
show databases;
-- @session
-- @bvt:issue

--内置表不能增删改
-- @bvt:issue#5707
update mo_catalog.mo_tables set relanme='mo_aaaa';
insert into mo_catalog.mo_role values (1763,'apple',0,1,'2022-09-22 06:53:34','');
delete from mo_catalog.mo_user;
drop table mo_catalog.mo_account;
delete from mo_catalog.mo_user_grant;
delete from mo_catalog.mo_role_grant;
delete from mo_catalog.mo_role_privs;
delete from mo_catalog.mo_database;
delete from mo_catalog.mo_columns;
-- @bvt:issue

--内置数据库不能删除
drop database information_schema;
drop database mo_catalog;
drop database system;
drop database system_metrics;

--moadmin,public删除/回收
revoke moadmin,public from root;
select count(*) from mo_catalog.mo_role_privs where role_name in ('moadmin','public');
-- @bvt:issue#5705
drop role if exists moadmin,public;
-- @bvt:issue
select role_name from mo_role where role_name in('moadmin','public');

--root/admin user修改/删除/授权
-- @bvt:issue#5705
drop user if exists admin,root;
-- @bvt:issue

--accountadmin删除/回收,切换到普通account验证
create account inner_account ADMIN_NAME 'admin' IDENTIFIED BY '111';
-- @session:id=2&user=inner_account:admin&password=123456
revoke accountadmin from admin;
select count(*) from mo_catalog.mo_role_privs where role_name in ('accountadmin');
-- @bvt:issue#5705
drop role if exists accountadmin;
-- @bvt:issue
select role_name from mo_catalog.mo_role where role_name in('accountadmin');
-- @session

drop account if exists account1;
drop account if exists inner_account;
drop role if exists revoke_role_1;