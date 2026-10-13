-- @suite
-- @setup
set @mode2_previous = @@global.lower_case_table_names;
set global lower_case_table_names = 2;

-- @session:id=1&user=sys:root&password=111
drop table if exists QaMode2Missing29422.t;
drop temporary table if exists QaMode2Missing29422.t;
drop database if exists QaMode2Child29422;
drop database if exists QaMode2Parent29422;
drop database if exists QaMode2Priv29422;
drop database if exists QaMode2Denied29422;
drop user if exists qa_mode2_29422_user;
drop role if exists qa_mode2_29422_role;

create database QaMode2Child29422;
use QaMode2Child29422;
drop table if exists AbsentT;
drop temporary table if exists AbsentT;
set foreign_key_checks = 0;
create table RefChild(id int primary key, pid int,
    constraint fk_ref_parent foreign key(pid) references qamode2parent29422.parent(id));
create table OtherChild(id int primary key, pid int,
    constraint fk_other_parent foreign key(pid) references qamode2unrelated29422.parent(id));
create database QaMode2Parent29422;
begin;
create table QaMode2Parent29422.Parent(id int primary key);
select refer_db_name = 'QaMode2Parent29422' as canonical_inside_txn
    from mo_catalog.mo_foreign_keys where db_name = 'QaMode2Child29422' and table_name = 'RefChild';
rollback;
select refer_db_name = 'qamode2parent29422' as alias_after_rollback
    from mo_catalog.mo_foreign_keys where db_name = 'QaMode2Child29422' and table_name = 'RefChild';
create table QaMode2Parent29422.Parent(id int primary key);
set foreign_key_checks = 1;
select refer_db_name = 'QaMode2Parent29422' and refer_table_name = 'Parent' as canonical_parent
    from mo_catalog.mo_foreign_keys where db_name = 'QaMode2Child29422' and table_name = 'RefChild';
select refer_db_name = 'qamode2unrelated29422' as unrelated_unchanged
    from mo_catalog.mo_foreign_keys where db_name = 'QaMode2Child29422' and table_name = 'OtherChild';
insert into RefChild values(1,99);
insert into QaMode2Parent29422.Parent values(5);
insert into RefChild values(2,5);
select id, pid from RefChild;
drop table RefChild;
drop table OtherChild;
drop database QaMode2Child29422;
drop database QaMode2Parent29422;

create database QaMode2Priv29422;
create database QaMode2Denied29422;
create table QaMode2Priv29422.PublicT(id int);
create table QaMode2Priv29422.RootT(id int);
create role qa_mode2_29422_role;
create user qa_mode2_29422_user identified by 'test123' default role qa_mode2_29422_role;
grant create table on database QaMode2Priv29422 to qa_mode2_29422_role;
grant select on table QaMode2Priv29422.PublicT to qa_mode2_29422_role;
-- @session

-- @session:id=2&user=sys:qa_mode2_29422_user:qa_mode2_29422_role&password=test123
select @@session.lower_case_table_names;
create table QaMode2Priv29422.ExactT(id int);
create table qamode2priv29422.AliasT(id int);
select count(*) from qamode2priv29422.publict;
begin;
create table qamode2priv29422.TxnAliasT(id int);
rollback;
drop table qamode2priv29422.exactt, qamode2priv29422.roott;
select count(*) from QaMode2Priv29422.ExactT;
drop table QaMode2Priv29422.ExactT, qamode2priv29422.aliast;
create table qamode2denied29422.DeniedT(id int);
-- @session

-- @session:id=3&user=sys:root&password=111
select count(*) from mo_catalog.mo_tables where reldatabase = 'QaMode2Priv29422' and relname in ('ExactT', 'AliasT');
select count(*) from mo_catalog.mo_tables where reldatabase = 'QaMode2Priv29422' and relname = 'TxnAliasT';
set global lower_case_table_names = 0;
-- @session

-- @session:id=4&user=sys:root&password=111
create database QaMode2Amb29422;
create database qamode2amb29422;
grant create table on database QaMode2Amb29422 to qa_mode2_29422_role;
-- @session
set global lower_case_table_names = 2;

-- @session:id=5&user=sys:qa_mode2_29422_user:qa_mode2_29422_role&password=test123
create table qamode2priv29422.WarmT(id int);
create table qamode2priv29422.Warm2T(id int);
create table qamode2amb29422.BadT(id int);
-- @session
set global lower_case_table_names = 0;

-- @session:id=6&user=sys:root&password=111
select count(*) from mo_catalog.mo_tables where relname = 'BadT' and reldatabase in ('QaMode2Amb29422', 'qamode2amb29422');
drop database QaMode2Amb29422;
drop database qamode2amb29422;
-- @session
set global lower_case_table_names = 2;

-- @session:id=7&user=sys:root&password=111
drop user qa_mode2_29422_user;
drop role qa_mode2_29422_role;
drop database QaMode2Priv29422;
drop database QaMode2Denied29422;
-- @session
set global lower_case_table_names = @mode2_previous;
