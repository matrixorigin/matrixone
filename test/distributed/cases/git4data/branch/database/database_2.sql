-- database-level data branch diff/merge/pick must reject users without database privilege
set global enable_privilege_cache = off;

drop database if exists db_branch_perm_parent;
drop database if exists db_branch_perm_src;
drop database if exists db_branch_perm_dst;
drop snapshot if exists db_branch_perm_sp_from;
drop snapshot if exists db_branch_perm_sp_to;
drop user if exists user_db_branch_low;
drop role if exists role_db_branch_low;

create database db_branch_perm_parent;
use db_branch_perm_parent;
create table features(id int primary key, score int);
insert into features values(1, 10);

data branch create database db_branch_perm_src from db_branch_perm_parent;
data branch create database db_branch_perm_dst from db_branch_perm_parent;
insert into db_branch_perm_src.features values(2, 20);

create snapshot db_branch_perm_sp_from for account sys;
insert into db_branch_perm_src.features values(3, 30);
create snapshot db_branch_perm_sp_to for account sys;

-- low-privilege user: only connect, no database privilege on the branch databases
create role role_db_branch_low;
create user user_db_branch_low identified by '111' default role role_db_branch_low;
grant role_db_branch_low to user_db_branch_low;
grant connect on account * to role_db_branch_low;

-- @session:id=1&user=user_db_branch_low&password=111
data branch diff database db_branch_perm_src against database db_branch_perm_dst;
data branch merge database db_branch_perm_src into database db_branch_perm_dst;
data branch pick database db_branch_perm_src into database db_branch_perm_dst between snapshot db_branch_perm_sp_from and db_branch_perm_sp_to;
-- @session

drop user user_db_branch_low;
drop role role_db_branch_low;
drop snapshot db_branch_perm_sp_from;
drop snapshot db_branch_perm_sp_to;
drop database db_branch_perm_src;
drop database db_branch_perm_dst;
drop database db_branch_perm_parent;

set global enable_privilege_cache = on;
