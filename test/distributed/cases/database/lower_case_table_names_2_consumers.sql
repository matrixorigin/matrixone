-- @suite
-- @setup
set @mode2_previous = @@global.lower_case_table_names;
set global lower_case_table_names = 2;

-- @session:id=1&user=sys:root&password=111
drop publication if exists qa_mode2_consumer_pub;
drop pitr if exists qa_mode2_consumer_pitr;
drop pitr if exists qa_mode2_consumer_internal internal;
drop snapshot if exists qa_mode2_consumer_snapshot;
drop database if exists QaMode2Consumer29422;
drop account if exists qa_mode2_consumer_sub;
create database QaMode2Consumer29422;
create table QaMode2Consumer29422.FinalMix(id int primary key, v int);
create table QaMode2Consumer29422.Secret(id int);
create table QaMode2Consumer29422.SecretPublic(id int);
insert into QaMode2Consumer29422.FinalMix values (1, 10);

-- Resolved metadata must use the physical table spelling in catalog predicates.
show full columns from qamode2consumer29422.finalmix;
show index from qamode2consumer29422.finalmix;
describe qamode2consumer29422.finalmix;
show create table qamode2consumer29422.finalmix;

-- Both data-branch entry points must store a physical name with its object ID.
create pitr qa_mode2_consumer_pitr for table qamode2consumer29422 finalmix range 1 'h';
select database_name, table_name, obj_id =
    (select rel_id from mo_catalog.mo_tables where reldatabase = 'QaMode2Consumer29422' and relname = 'FinalMix') as physical_id
    from mo_catalog.mo_pitr where pitr_name = 'qa_mode2_consumer_pitr';
-- @ignore:4
show recovery_window for table qamode2consumer29422 finalmix;
drop pitr qa_mode2_consumer_pitr;
create pitr qa_mode2_consumer_internal for table qamode2consumer29422 finalmix range 1 'h' internal;
select database_name, table_name, obj_id =
    (select rel_id from mo_catalog.mo_tables where reldatabase = 'QaMode2Consumer29422' and relname = 'FinalMix') as physical_id
    from mo_catalog.mo_pitr where pitr_name = 'qa_mode2_consumer_internal';
drop pitr qa_mode2_consumer_internal internal;
-- A copy-table ALTER changes the physical ID while retaining snapshot identity.
alter table QaMode2Consumer29422.FinalMix add column CopyMarker int first;
select rel_id <> rel_logical_id as copied from mo_catalog.mo_tables
    where reldatabase = 'QaMode2Consumer29422' and relname = 'FinalMix';
create snapshot qa_mode2_consumer_snapshot for table qamode2consumer29422 finalmix;
select database_name, table_name, obj_id =
    (select rel_logical_id from mo_catalog.mo_tables where reldatabase = 'QaMode2Consumer29422' and relname = 'FinalMix') as logical_id
    from mo_catalog.mo_snapshots where sname = 'qa_mode2_consumer_snapshot';
select v from qamode2consumer29422.finalmix{snapshot = 'qa_mode2_consumer_snapshot'};
-- @ignore:4
show recovery_window for table qamode2consumer29422 finalmix;
update QaMode2Consumer29422.FinalMix set v = 20;
restore table qamode2consumer29422.finalmix{snapshot = 'qa_mode2_consumer_snapshot'};
select v from QaMode2Consumer29422.FinalMix;
drop table QaMode2Consumer29422.FinalMix;
-- @ignore:4
show recovery_window for table qamode2consumer29422 finalmix;
restore table qamode2consumer29422.finalmix{snapshot = 'qa_mode2_consumer_snapshot'};
select v from QaMode2Consumer29422.FinalMix;
drop table QaMode2Consumer29422.FinalMix;
create table QaMode2Consumer29422.finalmix(id int primary key, v int);
insert into QaMode2Consumer29422.finalmix values (2, 30);
create table QaMode2Consumer29422.ChildRef(pid int,
    constraint fk_mode2_consumer foreign key(pid) references QaMode2Consumer29422.finalmix(id));
-- A current target with another physical spelling and a referencing FK must survive.
restore table qamode2consumer29422.finalmix{snapshot = 'qa_mode2_consumer_snapshot'};
select id, v from QaMode2Consumer29422.finalmix;
drop table QaMode2Consumer29422.ChildRef;
drop table QaMode2Consumer29422.finalmix;
restore table qamode2consumer29422.finalmix{snapshot = 'qa_mode2_consumer_snapshot'};
select v from QaMode2Consumer29422.FinalMix;
drop snapshot qa_mode2_consumer_snapshot;

create account qa_mode2_consumer_sub ADMIN_NAME 'admin' IDENTIFIED BY 'test123';
create publication qa_mode2_consumer_pub database QaMode2Consumer29422 table FinalMix,SecretPublic account qa_mode2_consumer_sub;
-- @session

-- @session:id=2&user=qa_mode2_consumer_sub:admin&password=test123
set global lower_case_table_names = 2;
-- @session

-- @session:id=3&user=qa_mode2_consumer_sub:admin&password=test123
select @@session.lower_case_table_names;
create database SubMix from sys publication qa_mode2_consumer_pub;
select id, v from SubMix.finalmix;
select id, v from submix.finalmix;
show create table SubMix.finalmix;
show create table submix.finalmix;
show create database submix;
select mo_table_rows('submix', 'finalmix');
create database qa_mode2_sub_view;
create view qa_mode2_sub_view.v as select id, v from submix.finalmix;
select id, v from qa_mode2_sub_view.v;
drop database qa_mode2_sub_view;
select id from SubMix.Secret;
select mo_table_rows('SubMix', 'Secret');
select mo_table_size('SubMix', 'Secret');
select count(*) from SYSTEM.STATEMENT_INFO where 1 = 0;
select count(*) from SYSTEM_METRICS.METRIC where 1 = 0;
select count(*) from MO_CATALOG.MO_TABLES where reldatabase = 'QaMode2Consumer29422';
select mo_table_col_max('mo_catalog', 'mo_tables', 'relname');
select mo_table_col_max('mo_catalog', 'MO_TABLES', 'relname');
select mo_table_col_min('mo_catalog', 'MO_TABLES', 'relname');
drop database SubMix;
-- @session

-- @session:id=4&user=sys:root&password=111
drop publication qa_mode2_consumer_pub;
drop account qa_mode2_consumer_sub;
drop database QaMode2Consumer29422;
-- @session

-- Recreating a dropped account commits the background transaction before the
-- remaining restore probes. The current-target resolver must reopen it safely.
drop snapshot if exists qa_mode2_account_restore_snapshot;
drop account if exists qa_mode2_account_restore;
create account qa_mode2_account_restore ADMIN_NAME 'admin' IDENTIFIED BY 'test123';
-- @session:id=5&user=qa_mode2_account_restore:admin&password=test123
set global lower_case_table_names = 2;
-- @session
-- @session:id=6&user=qa_mode2_account_restore:admin&password=test123
create database QaMode2AccountRestore;
create table QaMode2AccountRestore.Parent(id int primary key);
insert into QaMode2AccountRestore.Parent values (7);
-- @session
create snapshot qa_mode2_account_restore_snapshot for cluster;
drop account qa_mode2_account_restore;
select count(*) from mo_catalog.mo_database where datname = 'QaMode2AccountRestore';
select count(*) from mo_catalog.mo_tables where reldatabase = 'QaMode2AccountRestore';
restore account qa_mode2_account_restore{snapshot='qa_mode2_account_restore_snapshot'};
-- @session:id=7&user=qa_mode2_account_restore:admin&password=test123
-- Account recreation currently resets compatibility variables to defaults;
-- re-enable mode 2 to verify that the restored mixed-case data remains readable.
select @@session.lower_case_table_names;
set global lower_case_table_names = 2;
-- @session
-- @session:id=8&user=qa_mode2_account_restore:admin&password=test123
select id from QaMode2AccountRestore.Parent;
-- @session
drop snapshot qa_mode2_account_restore_snapshot;
drop account qa_mode2_account_restore;
select count(*) from mo_catalog.mo_database where datname = 'QaMode2AccountRestore';
select count(*) from mo_catalog.mo_tables where reldatabase = 'QaMode2AccountRestore';
set global lower_case_table_names = @mode2_previous;
