-- 隔离的角色规则、字符串子查询和认证回归；只使用最小数据。
drop user if exists rf_rule_user;
drop role if exists rf_rule_role;
drop database if exists rf_rule_db;
create database rf_rule_db;
create table rf_rule_db.documents (id int primary key, vbeln varchar(20));
create table rf_rule_db.allowed_documents (vbeln varchar(20), vkgrp varchar(8));
insert into rf_rule_db.documents values (1, ' 0001'), (2, '0002'), (3, '0003');
insert into rf_rule_db.allowed_documents values ('0001', ' 105 '), ('0001 ', '105'), ('0002', '106');
create role rf_rule_role;
alter role rf_rule_role add rule "select * from rf_rule_db.documents where trim(vbeln) in (select distinct trim(vbeln) from rf_rule_db.allowed_documents where trim(vkgrp) in ('105'))" on table rf_rule_db.documents;
show rules on role rf_rule_role;
create user rf_rule_user identified by 'rf_password' default role rf_rule_role;
grant connect on account * to rf_rule_role;
grant select on table rf_rule_db.* to rf_rule_role;

-- 正常登录后，实际角色会话只可见授权子查询匹配的行。
-- @session:id=1&user=sys:rf_rule_user:rf_rule_role&password=rf_password
set enable_remap_hint = 1;
select * from rf_rule_db.documents order by id;
select count(*) from rf_rule_db.allowed_documents where trim(vkgrp) = '105';
select * from rf_rule_db.documents order by id;
-- @session

-- 无效规则不能覆盖已生效的规则；后续新连接仍能正确认证并查询。
alter role rf_rule_role add rule "delete from rf_rule_db.documents" on table rf_rule_db.documents;
show rules on role rf_rule_role;
-- @session:id=2&user=sys:rf_rule_user:rf_rule_role&password=rf_password
set enable_remap_hint = 1;
select * from rf_rule_db.documents order by id;
-- @session

-- 规则更新为空集合后，新会话返回零行，而不是越权返回全部行。
alter role rf_rule_role add rule "select * from rf_rule_db.documents where trim(vbeln) in (select distinct trim(vbeln) from rf_rule_db.allowed_documents where trim(vkgrp) in ('999'))" on table rf_rule_db.documents;
-- @session:id=3&user=sys:rf_rule_user:rf_rule_role&password=rf_password
set enable_remap_hint = 1;
select * from rf_rule_db.documents order by id;
-- @session

-- 删除规则后重新登录，对照确认基础数据没有被破坏。
alter role rf_rule_role drop rule on table rf_rule_db.documents;
show rules on role rf_rule_role;
-- @session:id=4&user=sys:rf_rule_user:rf_rule_role&password=rf_password
set enable_remap_hint = 1;
select * from rf_rule_db.documents order by id;
-- @session

drop user rf_rule_user;
drop role rf_rule_role;
drop database rf_rule_db;
