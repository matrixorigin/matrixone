-- @suite
-- @setup
set @mode2_previous = @@global.lower_case_table_names;
set global lower_case_table_names = 2;

-- @session:id=1&user=sys:root&password=111
select @@session.lower_case_table_names;
drop publication if exists qa_mode2_issue29418_pub;
drop database if exists QaMode2Issue29418;
drop database if exists QaMode2AliasIssue29418;
create database QaMode2Issue29418;
select datname from mo_catalog.mo_database where account_id = 0 and datname = 'QaMode2Issue29418';
use QaMode2Issue29418;
create table T1 (id int);
show tables from QaMode2Issue29418;
set @first_id = (select dat_id from mo_catalog.mo_database where account_id = 0 and datname = 'QaMode2Issue29418');
drop schema if exists `QaMode2Issue29418`;
select count(*) from mo_catalog.mo_database where account_id = 0 and datname = 'QaMode2Issue29418';

create database QaMode2Issue29418;
select dat_id <> @first_id as new_physical_database from mo_catalog.mo_database where account_id = 0 and datname = 'QaMode2Issue29418';
begin;
drop database QaMode2Issue29418;
rollback;
select count(*) from mo_catalog.mo_database where account_id = 0 and datname = 'QaMode2Issue29418';
create publication qa_mode2_issue29418_pub database QaMode2Issue29418 account all;
drop database QaMode2Issue29418;
select count(*) from mo_catalog.mo_database where account_id = 0 and datname = 'QaMode2Issue29418';
drop publication qa_mode2_issue29418_pub;
drop database QaMode2Issue29418;
select count(*) from mo_catalog.mo_database where account_id = 0 and datname = 'QaMode2Issue29418';

create database QaMode2AliasIssue29418;
use qamode2aliasissue29418;
-- Resolve a new table through the transaction-local name index before commit.
begin;
create table TxnMix (id int);
insert into txnmix values (9);
select id from TXNMIX;
select relname from mo_catalog.mo_tables where reldatabase = 'QaMode2AliasIssue29418' and relname = 'TxnMix';
rollback;
select count(*) from mo_catalog.mo_tables where reldatabase = 'QaMode2AliasIssue29418' and relname = 'TxnMix';
create table MixT (id int comment 'id');
insert into mixt values (7);
select id from MIXT;
create index idx_mix_id on MIXT(id);
alter table mixt add column v int comment 'v';
show column_number from qamode2aliasissue29418.mixt;
show full columns from qamode2aliasissue29418.mixt;
show index from qamode2aliasissue29418.mixt;
show table_values from qamode2aliasissue29418.mixt;
select relname from mo_catalog.mo_tables where reldatabase = 'QaMode2AliasIssue29418' and relname = 'MixT';
drop index idx_mix_id on mixt;
rename table QaMode2AliasIssue29418.MixT to qamode2aliasissue29418.MixU;
alter table mixu rename to QAMODE2ALIASISSUE29418.MixT;
select relname from mo_catalog.mo_tables where reldatabase = 'QaMode2AliasIssue29418' and relname = 'MixT';
create view MixV as select id from MIXT;
alter view mixv as select id from mixt;
create or replace view MIXV as select id from mixt;
select id from mixv;
create sequence MixS increment 1 start with 1;
alter sequence mixs increment 2;
select relname from mo_catalog.mo_tables where reldatabase = 'QaMode2AliasIssue29418' and relname = 'MixS';
drop sequence mixs;
drop view mixv;
create temporary table TempMix(id int primary key, v int);
insert into tempmix values (1, 2);
create index idx_temp_v on TEMPMIX(v);
alter table tempmix add column x int;
select id, v, x from TEMPMIX;
truncate table tEMpMix;
select count(*) from tempmix;
drop index idx_temp_v on TEMPMIX;
drop temporary table tempmix;
set foreign_key_checks = 0;
create table ForwardChild(id int primary key, pid int, constraint fk_forward_parent foreign key(pid) references forwardparent(id));
create table ForwardParent(id int primary key);
select refer_table_name from mo_catalog.mo_foreign_keys where db_name = 'QaMode2AliasIssue29418' and table_name = 'ForwardChild';
drop table forwardchild;
drop table forwardparent;
set foreign_key_checks = 1;
show tables from QAMODE2ALIASISSUE29418;
create publication qa_mode2_issue29418_table_pub database qamode2aliasissue29418 table mixt,MIXT account all;
select table_list = 'MixT' as canonical_table from mo_catalog.mo_pubs where pub_name = 'qa_mode2_issue29418_table_pub';
create database QaMode2PubTargetIssue29418;
create table QaMode2PubTargetIssue29418.NextT(id int);
alter publication qa_mode2_issue29418_table_pub database qamode2pubtargetissue29418 table nextt;
select database_name = 'QaMode2PubTargetIssue29418' and table_list = 'NextT' as canonical_target from mo_catalog.mo_pubs where pub_name = 'qa_mode2_issue29418_table_pub';
alter publication qa_mode2_issue29418_table_pub database QAMODE2ALIASISSUE29418 table MIXT;
select database_name = 'QaMode2AliasIssue29418' and table_list = 'MixT' as canonical_target from mo_catalog.mo_pubs where pub_name = 'qa_mode2_issue29418_table_pub';
drop publication qa_mode2_issue29418_table_pub;
select rel_id <> rel_logical_id as new_physical_generation from mo_catalog.mo_tables
    where reldatabase = 'QaMode2AliasIssue29418' and relname = 'MixT';
create pitr qa29418_frontend_pitr for table qamode2aliasissue29418 mixt range 1 'h';
select p.database_name = t.reldatabase and p.table_name = t.relname as physical_pitr
    from mo_catalog.mo_pitr p left join mo_catalog.mo_tables t on p.obj_id = t.rel_id
    where p.pitr_name = 'qa29418_frontend_pitr';
create pitr qa29418_duplicate_pitr for table QAMODE2ALIASISSUE29418 MIXT range 1 'h';
drop pitr qa29418_frontend_pitr;
create pitr qa29418_internal_pitr for table qamode2aliasissue29418 mixt range 1 'h' internal;
select p.database_name = t.reldatabase and p.table_name = t.relname as physical_pitr
    from mo_catalog.mo_pitr p left join mo_catalog.mo_tables t on p.obj_id = t.rel_id
    where p.pitr_name = 'qa29418_internal_pitr';
drop pitr qa29418_internal_pitr;
create table PitrDropT(id int);
alter table pitrDropT add column v int;
create pitr qa29418_drop_pitr for table qamode2aliasissue29418 pitrDropT range 1 'h';
select p.obj_id = t.rel_id as physical_pitr from mo_catalog.mo_pitr p
    left join mo_catalog.mo_tables t on p.obj_id = t.rel_id where p.pitr_name = 'qa29418_drop_pitr';
drop table PITRDROPT;
select pitr_status = 0 as expired_after_drop from mo_catalog.mo_pitr where pitr_name = 'qa29418_drop_pitr';
drop pitr qa29418_drop_pitr;
create table ViewSrc(code varchar(5));
create view ViewAlias as select code from ViewSrc;
alter table viewsrc modify column code varchar(60);
select column_name, character_maximum_length from information_schema.columns
    where table_schema = 'QaMode2AliasIssue29418' and table_name = 'ViewAlias';
drop view viewalias;
drop table viewsrc;
drop database QaMode2PubTargetIssue29418;
drop database qamode2aliasissue29418;
select count(*) from mo_catalog.mo_database where account_id = 0 and datname = 'QaMode2AliasIssue29418';
-- @session

create account qa29418owner ADMIN_NAME 'admin' IDENTIFIED BY 'test123';
-- @session:id=5&user=qa29418owner:admin&password=test123
set global lower_case_table_names = 2;
-- @session
-- @session:id=6&user=qa29418owner:admin&password=test123
create database Qa29418Owned;
create table Qa29418Owned.OwnedT(id int);
-- @session
-- @session:id=7&user=sys:root&password=111
create database Qa29418Read;
create table Qa29418Read.MixedT(id int primary key, v int, index idx_v(v));
insert into Qa29418Read.MixedT values (1, 7);
create table Qa29418Read.HiddenT(id int);
create publication qa29418_read_pub database Qa29418Read table MixedT account qa29418owner;
create publication qa29418_cross_owner_pub database qa29418owned table ownedt account qa29418owner;
select database_name = 'Qa29418Owned' and table_list = 'OwnedT' and database_id <> 0 as physical_owner_target from mo_catalog.mo_pubs where pub_name = 'qa29418_cross_owner_pub';
-- @session:id=8&user=qa29418owner:admin&password=test123
set global lower_case_table_names = 2;
-- @session:id=9&user=qa29418owner:admin&password=test123
select @@session.lower_case_table_names;
create database Sub29418 from sys publication qa29418_read_pub;
select id, v from sub29418.mixedt;
show column_number from sub29418.mixedt;
show table_values from sub29418.mixedt;
select mo_table_rows('Sub29418', 'MixedT') = mo_table_rows('sub29418', 'mixedt') as same_rows;
create table sub29418.forbidden_t(id int);
create index forbidden_idx on sub29418.mixedt(v);
select * from sub29418.hiddent;
select 1 from SYSTEM.STATEMENT_INFO limit 0;
select 1 from SYSTEM_METRICS.METRIC limit 0;
select mo_table_col_max('mo_catalog', 'mo_database', 'dat_id');
select mo_table_col_max('MO_CATALOG', 'MO_DATABASE', 'dat_id');
drop database Sub29418;
-- @session:id=7&user=sys:root&password=111
drop publication qa29418_read_pub;
drop publication qa29418_cross_owner_pub;
drop database Qa29418Read;
-- Legacy physical names, including folded collisions and quoted identifiers, must all be
-- retired even when SYS's DROP ACCOUNT runs in mode 2.
-- @session:id=10&user=qa29418owner:admin&password=test123
set global lower_case_table_names = 0;
-- @session
-- @session:id=11&user=qa29418owner:admin&password=test123
create database Qa29418Collision;
create database qa29418collision;
create database `Qa29418-Ä`;
create table `Qa29418-Ä`.T(id int);
-- @session:id=7&user=sys:root&password=111
drop account qa29418owner;
select count(*) from mo_catalog.mo_database where lower(datname) like 'qa29418%';
select count(*) from mo_catalog.mo_tables where lower(reldatabase) like 'qa29418%';
-- @session

set global lower_case_table_names = 1;

-- Existing mode-0 collisions must fail closed for every spelling in mode 2.
set global lower_case_table_names = 0;
-- @session:id=2&user=sys:root&password=111
drop database if exists QaMode2Collision29418;
drop database if exists qamode2collision29418;
create database QaMode2Collision29418;
create database qamode2collision29418;
-- @session
set global lower_case_table_names = 2;
-- @session:id=3&user=sys:root&password=111
use QaMode2Collision29418;
use qamode2collision29418;
drop database if exists QaMode2Collision29418;
-- @session
set global lower_case_table_names = 0;
-- @session:id=4&user=sys:root&password=111
drop database QaMode2Collision29418;
drop database qamode2collision29418;
-- @session
set global lower_case_table_names = @mode2_previous;
