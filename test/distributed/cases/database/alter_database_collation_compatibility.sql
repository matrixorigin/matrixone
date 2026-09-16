drop database if exists alter_database_collation_compat;
create database alter_database_collation_compat;
alter database `alter_database_collation_compat` character set utf8mb4 collate utf8mb4_bin;
use alter_database_collation_compat;
select database();
drop database alter_database_collation_compat;
