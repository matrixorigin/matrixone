-- Database CLONE follows the bulk-restore policy for external tables: the
-- external relation is skipped, while ordinary tables and the target database
-- are still cloned successfully.
-- Allow the previous test run's asynchronous DDL cleanup to finish before
-- reusing names.
-- @sleep:2
drop snapshot if exists issue_27103_snapshot;
drop database if exists issue_27103_live_src;
drop database if exists issue_27103_live_dst;
drop database if exists issue_27103_snapshot_src;
drop database if exists issue_27103_snapshot_dst;
drop database if exists issue_27103_external_only_src;
drop database if exists issue_27103_external_only_dst;

create database issue_27103_live_src;
create table issue_27103_live_src.control_t(id int primary key, payload varchar(20));
insert into issue_27103_live_src.control_t values (1, 'ordinary');
create external table issue_27103_live_src.ext_t(id int, payload varchar(32))
infile{"filepath"='$resources/external_table_file/aaa.csv','format'='csv'}
fields terminated by ',';
create view issue_27103_live_src.ext_v as select * from issue_27103_live_src.ext_t;
select count(*) as source_external_rows from issue_27103_live_src.ext_t;

create database issue_27103_live_dst clone issue_27103_live_src;
select count(*) as live_target_database from mo_catalog.mo_database
where datname = 'issue_27103_live_dst';
show tables from issue_27103_live_dst;
select * from issue_27103_live_dst.control_t order by id;
select count(*) as live_target_external_tables from mo_catalog.mo_tables
where reldatabase = 'issue_27103_live_dst' and relkind = 'e';
select count(*) as live_target_external_views from mo_catalog.mo_tables
where reldatabase = 'issue_27103_live_dst' and relkind = 'v' and relname = 'ext_v';
select count(*) as live_target_control_tables from mo_catalog.mo_tables
where reldatabase = 'issue_27103_live_dst' and relname = 'control_t' and relkind = 'r';
select count(*) as source_external_rows_after_clone from issue_27103_live_src.ext_t;

create database issue_27103_snapshot_src;
create table issue_27103_snapshot_src.control_t(id int primary key, payload varchar(20));
insert into issue_27103_snapshot_src.control_t values (2, 'snapshot');
create external table issue_27103_snapshot_src.ext_t(id int, payload varchar(32))
infile{"filepath"='$resources/external_table_file/aaa.csv','format'='csv'}
fields terminated by ',';
create view issue_27103_snapshot_src.ext_v as select * from issue_27103_snapshot_src.ext_t;
select count(*) as snapshot_source_external_rows from issue_27103_snapshot_src.ext_t;
create snapshot issue_27103_snapshot for database issue_27103_snapshot_src;
drop database issue_27103_snapshot_src;

create database issue_27103_snapshot_dst clone issue_27103_snapshot_src
{snapshot = "issue_27103_snapshot"};
select count(*) as snapshot_target_database from mo_catalog.mo_database
where datname = 'issue_27103_snapshot_dst';
show tables from issue_27103_snapshot_dst;
select * from issue_27103_snapshot_dst.control_t order by id;
select count(*) as snapshot_target_external_tables from mo_catalog.mo_tables
where reldatabase = 'issue_27103_snapshot_dst' and relkind = 'e';
select count(*) as snapshot_target_external_views from mo_catalog.mo_tables
where reldatabase = 'issue_27103_snapshot_dst' and relkind = 'v' and relname = 'ext_v';
select count(*) as snapshot_target_control_tables from mo_catalog.mo_tables
where reldatabase = 'issue_27103_snapshot_dst' and relname = 'control_t' and relkind = 'r';

create database issue_27103_external_only_src;
create external table issue_27103_external_only_src.ext_t(id int, payload varchar(32))
infile{"filepath"='$resources/external_table_file/aaa.csv','format'='csv'}
fields terminated by ',';
create database issue_27103_external_only_dst clone issue_27103_external_only_src;
select count(*) as external_only_target_database from mo_catalog.mo_database
where datname = 'issue_27103_external_only_dst';
select count(*) as external_only_target_tables from mo_catalog.mo_tables
where reldatabase = 'issue_27103_external_only_dst';

drop snapshot if exists issue_27103_snapshot;
drop database if exists issue_27103_live_src;
drop database if exists issue_27103_live_dst;
drop database if exists issue_27103_snapshot_src;
drop database if exists issue_27103_snapshot_dst;
drop database if exists issue_27103_external_only_src;
drop database if exists issue_27103_external_only_dst;
