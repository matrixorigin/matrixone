drop database if exists alter_database_collation_compat;
create database alter_database_collation_compat;
alter database `alter_database_collation_compat` character set utf8mb4 collate utf8mb4_bin;
create role alter_database_collation_role;
grant connect on account * to alter_database_collation_role;
create user alter_database_collation_user identified by '111' default role alter_database_collation_role;
-- @session:id=1&user=sys:alter_database_collation_user:alter_database_collation_role&password=111
alter database `alter_database_collation_compat` character set utf8mb4 collate utf8mb4_bin;
-- @session
grant ownership on database alter_database_collation_compat to alter_database_collation_role;
-- @session:id=1&user=sys:alter_database_collation_user:alter_database_collation_role&password=111
alter database `alter_database_collation_compat` character set utf8mb4 collate utf8mb4_bin;
-- @session
use alter_database_collation_compat;
select database();
alter database missing_alter_database_collation_compat character set utf8mb4 collate utf8mb4_bin;
alter database alter_database_collation_compat character set utf8mb4 collate utf8mb4_general_ci;
create table t(a int);
begin;
insert into t values (1);
alter database `alter_database_collation_compat` character set utf8mb4 collate utf8mb4_bin;
commit;
select count(*) from t;
prepare alter_database_collation_stmt from 'alter database `alter_database_collation_compat` character set utf8mb4 collate utf8mb4_bin';
execute alter_database_collation_stmt;
deallocate prepare alter_database_collation_stmt;
drop user alter_database_collation_user;
drop role alter_database_collation_role;
drop database alter_database_collation_compat;
