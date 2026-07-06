set global enable_privilege_cache = off;

drop account if exists acc_branch_priv;
drop account if exists `acc-branch`;
drop database if exists br_sys_src;
drop publication if exists pub_br_sub;
drop database if exists br_pub_src;
drop snapshot if exists sp_to_account;
create account acc_branch_priv admin_name "admin" identified by "111";
create account `acc-branch` admin_name "rootq" identified by "111";

-- Quoted TO ACCOUNT identifiers must round-trip through internal clone SQL.
create database br_sys_src;
create table br_sys_src.t(a int primary key);
insert into br_sys_src.t values (1);
create snapshot sp_to_account for database br_sys_src;
data branch create database br_to_acc from br_sys_src{snapshot="sp_to_account"} to account `acc-branch`;
set @quoted_acc_id = (select account_id from mo_catalog.mo_account where account_name = 'acc-branch');
select count(*) from mo_catalog.mo_database where account_id = @quoted_acc_id and datname = 'br_to_acc';
select count(*) from mo_catalog.mo_tables where account_id = @quoted_acc_id and reldatabase = 'br_to_acc' and relname = 't';
drop snapshot sp_to_account;
drop database br_sys_src;

-- Subscription sources must use the same CREATE DATABASE + source SELECT authorization.
create database br_pub_src;
create table br_pub_src.t1(a int primary key);
insert into br_pub_src.t1 values (1), (2);
create publication pub_br_sub database br_pub_src account acc_branch_priv;

-- @session:id=1&user=acc_branch_priv:admin&password=111
drop database if exists br_src;
drop database if exists br_diff;
drop database if exists br_merge;
drop database if exists br_pick;
drop database if exists br_delete;
drop database if exists br_db_ok;
drop database if exists br_db_normal;
drop database if exists br_db_branch;
drop database if exists br_db_empty;
drop database if exists br_db_view_only;
drop database if exists br_db_sequence;
drop database if exists br_db_mixed;
drop database if exists sub_br_pub;
drop database if exists br_sub_ok;

create database br_src;
create table br_src.t(a int primary key, b varchar(20));
insert into br_src.t values (1, 'one'), (2, 'two');
create snapshot sp_t for table br_src t;
create snapshot sp_db for database br_src;

create role r_tbl_select_only;
grant select on table br_src.t to r_tbl_select_only;
create user u_tbl_select identified by '111' default role r_tbl_select_only;

create role r_tbl_create_only;
grant create table on database br_src to r_tbl_create_only;
create user u_tbl_create identified by '111' default role r_tbl_create_only;

create role r_tbl_both;
grant select on table br_src.t to r_tbl_both;
grant create table on database br_src to r_tbl_both;
create user u_tbl_both identified by '111' default role r_tbl_both;

create role r_db_select_only;
grant select on table br_src.t to r_db_select_only;
create user u_db_select identified by '111' default role r_db_select_only;

create role r_db_create_only;
grant create database on account * to r_db_create_only;
create user u_db_create identified by '111' default role r_db_create_only;

create role r_db_both;
grant create database on account * to r_db_both;
grant select on table br_src.t to r_db_both;
create user u_db_both identified by '111' default role r_db_both;

create database sub_br_pub from sys publication pub_br_sub;
show full tables from sub_br_pub;
-- @session

-- DATA BRANCH CREATE TABLE requires both source SELECT and target CREATE TABLE.
-- @session:id=2&user=acc_branch_priv:u_tbl_select:r_tbl_select_only&password=111
data branch create table br_src.t_select_only from br_src.t{snapshot="sp_t"};
-- @session

-- @session:id=3&user=acc_branch_priv:u_tbl_create:r_tbl_create_only&password=111
data branch create table br_src.t_create_only from br_src.t{snapshot="sp_t"};
-- @session

-- @session:id=1&user=acc_branch_priv:admin&password=111
select count(*) from mo_catalog.mo_tables where reldatabase = 'br_src' and relname in ('t_select_only', 't_create_only');
-- @session

-- @session:id=4&user=acc_branch_priv:u_tbl_both:r_tbl_both&password=111
data branch create table br_src.t_branch_ok from br_src.t{snapshot="sp_t"};
-- @session

-- @session:id=1&user=acc_branch_priv:admin&password=111
select count(*) from br_src.t_branch_ok;
-- @session

-- DATA BRANCH CREATE DATABASE requires CREATE DATABASE plus source read.
-- @session:id=5&user=acc_branch_priv:u_db_select:r_db_select_only&password=111
data branch create database br_db_select_only from br_src{snapshot="sp_db"};
-- @session

-- @session:id=6&user=acc_branch_priv:u_db_create:r_db_create_only&password=111
data branch create database br_db_create_only from br_src{snapshot="sp_db"};
-- @session

-- @session:id=1&user=acc_branch_priv:admin&password=111
select count(*) from mo_catalog.mo_database where datname in ('br_db_select_only', 'br_db_create_only');
-- @session

-- @session:id=7&user=acc_branch_priv:u_db_both:r_db_both&password=111
data branch create database br_db_ok from br_src{snapshot="sp_db"};
-- @session

-- @session:id=1&user=acc_branch_priv:admin&password=111
select count(*) from br_db_ok.t;
-- @session

-- @session:id=24&user=acc_branch_priv:u_db_create:r_db_create_only&password=111
data branch create database br_sub_create_only from sub_br_pub;
-- @session

-- @session:id=1&user=acc_branch_priv:admin&password=111
data branch create database br_sub_ok from sub_br_pub;
select count(*) from mo_catalog.mo_database where datname = 'br_sub_create_only';
select count(*) from br_sub_ok.t1;

create database br_diff;
create table br_diff.base(a int primary key, b int);
create table br_diff.target(a int primary key, b int);
insert into br_diff.base values (1, 1), (2, 2);
insert into br_diff.target values (1, 10), (3, 3);

create role r_diff_one_side;
grant select on table br_diff.target to r_diff_one_side;
create user u_diff_one identified by '111' default role r_diff_one_side;

create role r_diff_both;
grant select on table br_diff.base to r_diff_both;
grant select on table br_diff.target to r_diff_both;
create user u_diff_both identified by '111' default role r_diff_both;
-- @session

-- DATA BRANCH DIFF requires read on both sides.
-- @session:id=8&user=acc_branch_priv:u_diff_one:r_diff_one_side&password=111
data branch diff br_diff.target against br_diff.base output count;
-- @session

-- @session:id=9&user=acc_branch_priv:u_diff_both:r_diff_both&password=111
data branch diff br_diff.target against br_diff.base output count;
data branch diff br_diff.target against br_diff.base output as br_diff.diff_out;
-- @session

-- @session:id=1&user=acc_branch_priv:admin&password=111
select count(*) from mo_catalog.mo_tables where reldatabase = 'br_diff' and relname = 'diff_out';

create database br_merge;
create table br_merge.src(a int primary key, b int);
create table br_merge.dst(a int primary key, b int);
insert into br_merge.src values (1, 10), (2, 20);
insert into br_merge.dst values (1, 10);

create role r_merge_no_src;
grant select on table br_merge.dst to r_merge_no_src;
grant insert on table br_merge.dst to r_merge_no_src;
grant update on table br_merge.dst to r_merge_no_src;
grant delete on table br_merge.dst to r_merge_no_src;
create user u_merge_no_src identified by '111' default role r_merge_no_src;

create role r_merge_no_write;
grant select on table br_merge.src to r_merge_no_write;
grant select on table br_merge.dst to r_merge_no_write;
create user u_merge_no_write identified by '111' default role r_merge_no_write;

create role r_merge_no_insert;
grant select on table br_merge.src to r_merge_no_insert;
grant select on table br_merge.dst to r_merge_no_insert;
grant update on table br_merge.dst to r_merge_no_insert;
grant delete on table br_merge.dst to r_merge_no_insert;
create user u_merge_no_insert identified by '111' default role r_merge_no_insert;

create role r_merge_no_update;
grant select on table br_merge.src to r_merge_no_update;
grant select on table br_merge.dst to r_merge_no_update;
grant insert on table br_merge.dst to r_merge_no_update;
grant delete on table br_merge.dst to r_merge_no_update;
create user u_merge_no_update identified by '111' default role r_merge_no_update;

create role r_merge_no_delete;
grant select on table br_merge.src to r_merge_no_delete;
grant select on table br_merge.dst to r_merge_no_delete;
grant insert on table br_merge.dst to r_merge_no_delete;
grant update on table br_merge.dst to r_merge_no_delete;
create user u_merge_no_delete identified by '111' default role r_merge_no_delete;

create role r_merge_all;
grant select on table br_merge.src to r_merge_all;
grant select on table br_merge.dst to r_merge_all;
grant insert on table br_merge.dst to r_merge_all;
grant update on table br_merge.dst to r_merge_all;
grant delete on table br_merge.dst to r_merge_all;
create user u_merge_all identified by '111' default role r_merge_all;
-- @session

-- DATA BRANCH MERGE requires source read, destination read, and destination writes.
-- @session:id=10&user=acc_branch_priv:u_merge_no_src:r_merge_no_src&password=111
data branch merge br_merge.src into br_merge.dst;
-- @session

-- @session:id=11&user=acc_branch_priv:u_merge_no_write:r_merge_no_write&password=111
data branch merge br_merge.src into br_merge.dst;
-- @session

-- @session:id=12&user=acc_branch_priv:u_merge_no_insert:r_merge_no_insert&password=111
data branch merge br_merge.src into br_merge.dst;
-- @session

-- @session:id=16&user=acc_branch_priv:u_merge_no_update:r_merge_no_update&password=111
data branch merge br_merge.src into br_merge.dst;
-- @session

-- @session:id=17&user=acc_branch_priv:u_merge_no_delete:r_merge_no_delete&password=111
data branch merge br_merge.src into br_merge.dst;
-- @session

-- @session:id=1&user=acc_branch_priv:admin&password=111
select * from br_merge.dst order by a;
-- @session

-- @session:id=18&user=acc_branch_priv:u_merge_all:r_merge_all&password=111
data branch merge br_merge.src into br_merge.dst;
-- @session

-- @session:id=1&user=acc_branch_priv:admin&password=111
select * from br_merge.dst order by a;

create database br_pick;
create table br_pick.src(a int primary key, b int);
create table br_pick.dst(a int primary key, b int);
create table br_pick.pick_keys(k int);
insert into br_pick.src values (1, 10), (2, 20);
insert into br_pick.dst values (1, 1);
insert into br_pick.pick_keys values (2);

create role r_pick_no_key;
grant select on table br_pick.src to r_pick_no_key;
grant select on table br_pick.dst to r_pick_no_key;
grant insert on table br_pick.dst to r_pick_no_key;
grant update on table br_pick.dst to r_pick_no_key;
grant delete on table br_pick.dst to r_pick_no_key;
create user u_pick_no_key identified by '111' default role r_pick_no_key;

create role r_pick_no_insert;
grant select on table br_pick.src to r_pick_no_insert;
grant select on table br_pick.dst to r_pick_no_insert;
grant select on table br_pick.pick_keys to r_pick_no_insert;
grant update on table br_pick.dst to r_pick_no_insert;
grant delete on table br_pick.dst to r_pick_no_insert;
create user u_pick_no_insert identified by '111' default role r_pick_no_insert;

create role r_pick_no_update;
grant select on table br_pick.src to r_pick_no_update;
grant select on table br_pick.dst to r_pick_no_update;
grant select on table br_pick.pick_keys to r_pick_no_update;
grant insert on table br_pick.dst to r_pick_no_update;
grant delete on table br_pick.dst to r_pick_no_update;
create user u_pick_no_update identified by '111' default role r_pick_no_update;

create role r_pick_no_delete;
grant select on table br_pick.src to r_pick_no_delete;
grant select on table br_pick.dst to r_pick_no_delete;
grant select on table br_pick.pick_keys to r_pick_no_delete;
grant insert on table br_pick.dst to r_pick_no_delete;
grant update on table br_pick.dst to r_pick_no_delete;
create user u_pick_no_delete identified by '111' default role r_pick_no_delete;

create role r_pick_all;
grant select on table br_pick.src to r_pick_all;
grant select on table br_pick.dst to r_pick_all;
grant select on table br_pick.pick_keys to r_pick_all;
grant insert on table br_pick.dst to r_pick_all;
grant update on table br_pick.dst to r_pick_all;
grant delete on table br_pick.dst to r_pick_all;
create user u_pick_all identified by '111' default role r_pick_all;
-- @session

-- DATA BRANCH PICK keeps normal authorization for KEYS subqueries.
-- @session:id=13&user=acc_branch_priv:u_pick_no_key:r_pick_no_key&password=111
data branch pick br_pick.src into br_pick.dst keys(select k from br_pick.pick_keys);
-- @session

-- @session:id=19&user=acc_branch_priv:u_pick_no_insert:r_pick_no_insert&password=111
data branch pick br_pick.src into br_pick.dst keys(select k from br_pick.pick_keys);
-- @session

-- @session:id=20&user=acc_branch_priv:u_pick_no_update:r_pick_no_update&password=111
data branch pick br_pick.src into br_pick.dst keys(select k from br_pick.pick_keys);
-- @session

-- @session:id=21&user=acc_branch_priv:u_pick_no_delete:r_pick_no_delete&password=111
data branch pick br_pick.src into br_pick.dst keys(select k from br_pick.pick_keys);
-- @session

-- @session:id=1&user=acc_branch_priv:admin&password=111
select * from br_pick.dst order by a;
-- @session

-- @session:id=22&user=acc_branch_priv:u_pick_all:r_pick_all&password=111
data branch pick br_pick.src into br_pick.dst keys(select k from br_pick.pick_keys);
-- @session

-- @session:id=1&user=acc_branch_priv:admin&password=111
select * from br_pick.dst order by a;

create database br_delete;
create table br_delete.base(a int primary key);
insert into br_delete.base values (1);
data branch create table br_delete.branch_t from br_delete.base;
create table br_delete.normal_t(a int primary key);

create database br_db_normal;
create table br_db_normal.normal_t(a int primary key);

create database br_db_wildcard;
create table br_db_wildcard.zzmoatmpb_normal(a int primary key);

create database br_db_empty;

create database br_db_view_only;
create view br_db_view_only.v as select 1 as a;

create database br_db_sequence;
create sequence br_db_sequence.s;

create database br_db_mixed;
create table br_db_mixed.base(a int primary key);
insert into br_db_mixed.base values (1);
data branch create table br_db_mixed.branch_t from br_db_mixed.base;
create table br_db_mixed.normal_t(a int primary key);

create database br_db_src;
create table br_db_src.t(a int primary key);
insert into br_db_src.t values (1);
data branch create database br_db_branch from br_db_src;

create role r_branch_drop;
grant drop table on database br_delete to r_branch_drop;
grant drop database on account * to r_branch_drop;
create user u_branch_drop identified by '111' default role r_branch_drop;
-- @session

-- DATA BRANCH DELETE cannot delete ordinary non-branch targets.
-- @session:id=15&user=acc_branch_priv:u_branch_drop:r_branch_drop&password=111
data branch delete table br_delete.normal_t;
data branch delete table br_delete.branch_t;
data branch delete database br_db_normal;
data branch delete database br_db_wildcard;
data branch delete database br_db_empty;
data branch delete database br_db_view_only;
data branch delete database br_db_sequence;
data branch delete database br_db_mixed;
data branch delete database br_db_branch;
-- @session

-- @session:id=1&user=acc_branch_priv:admin&password=111
select count(*) from mo_catalog.mo_tables where reldatabase = 'br_delete' and relname = 'normal_t';
select count(*) from mo_catalog.mo_tables where reldatabase = 'br_delete' and relname = 'branch_t';
select count(*) from mo_catalog.mo_database where datname = 'br_db_normal';
select count(*) from mo_catalog.mo_database where datname = 'br_db_wildcard';
select count(*) from mo_catalog.mo_tables where reldatabase = 'br_db_wildcard' and relname = 'zzmoatmpb_normal';
select count(*) from mo_catalog.mo_database where datname = 'br_db_empty';
select count(*) from mo_catalog.mo_database where datname = 'br_db_view_only';
select count(*) from mo_catalog.mo_database where datname = 'br_db_sequence';
select count(*) from mo_catalog.mo_database where datname = 'br_db_mixed';
select count(*) from mo_catalog.mo_tables where reldatabase = 'br_db_mixed' and relname = 'branch_t';
select count(*) from mo_catalog.mo_tables where reldatabase = 'br_db_mixed' and relname = 'normal_t';
select count(*) from mo_catalog.mo_database where datname = 'br_db_branch';
-- @session

set global enable_privilege_cache = on;

-- DATA BRANCH privilege checks also work while privilege cache is enabled.
-- @session:id=23&user=acc_branch_priv:u_diff_both:r_diff_both&password=111
data branch diff br_diff.target against br_diff.base output count;
-- @session

-- @session:id=1&user=acc_branch_priv:admin&password=111
drop database if exists br_sub_ok;
drop database if exists sub_br_pub;
-- @session

drop publication if exists pub_br_sub;
drop database if exists br_pub_src;
drop account if exists acc_branch_priv;
drop account if exists `acc-branch`;
set global enable_privilege_cache = on;
