-- Missing-object error contracts for DROP statements (#31451, #31735):
-- a missing DROP TABLE target reports MySQL 1051 (Unknown table) and a
-- missing index on an existing table reports MySQL 1091 (Can't DROP).
-- SELECT/TRUNCATE missing lookups keep their own classification.
drop database if exists drop_missing_contract_db;
create database drop_missing_contract_db;
use drop_missing_contract_db;
drop table missing_t1;
create table t1(a int primary key);
drop index missing_idx on t1;
drop table if exists missing_t1;
drop index if exists missing_idx on t1;
create table t2(a int primary key, b int);
create index idx_b on t2(b);
alter table t2 drop column b;
drop index idx_b on t2;
select * from missing_t1;
truncate table missing_t1;
drop index any_idx on missing_t2;
drop table t1;
drop table t2;
drop database drop_missing_contract_db;
