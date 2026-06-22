set global enable_privilege_cache = off;

drop user if exists replace_priv_user;
drop user if exists replace_priv_delete_user;
drop role if exists replace_priv_role;
drop role if exists replace_priv_delete_role;
drop database if exists replace_priv_edge;

create database replace_priv_edge;
create table replace_priv_edge.t_priv(id int primary key, v int);
insert into replace_priv_edge.t_priv values(1, 10);
create table replace_priv_edge.t_unique(id int, v int unique);
insert into replace_priv_edge.t_unique values(1, 10);
create table replace_priv_edge.t_insert_only(id int, v int);

create user replace_priv_user identified by '123456';
create user replace_priv_delete_user identified by '123456';
create role replace_priv_role;
create role replace_priv_delete_role;
grant select, insert on table replace_priv_edge.* to replace_priv_role;
grant select, insert, delete on table replace_priv_edge.* to replace_priv_delete_role;
grant replace_priv_role to replace_priv_user;
grant replace_priv_delete_role to replace_priv_delete_user;

-- @session:id=1&user=sys:replace_priv_user:replace_priv_role&password=123456
replace into replace_priv_edge.t_priv values(1, 20);
replace into replace_priv_edge.t_unique values(2, 10);
select * from replace_priv_edge.t_priv order by id;
select * from replace_priv_edge.t_unique order by id;
replace into replace_priv_edge.t_insert_only values(1, 100);
select * from replace_priv_edge.t_insert_only order by id;
-- @session

-- @session:id=2&user=sys:replace_priv_delete_user:replace_priv_delete_role&password=123456
replace into replace_priv_edge.t_priv values(1, 30);
replace into replace_priv_edge.t_unique values(2, 10);
select * from replace_priv_edge.t_priv order by id;
select * from replace_priv_edge.t_unique order by id;
-- @session

drop user replace_priv_user;
drop user replace_priv_delete_user;
drop role replace_priv_role;
drop role replace_priv_delete_role;
drop database replace_priv_edge;
