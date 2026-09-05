-- Database CLONE follows the bulk-restore policy for external tables: the
-- external relation is skipped, while ordinary tables and the target database
-- are still cloned successfully.
-- Poll catalog state after the previous run's cleanup before reusing names.
drop account if exists issue_27103_branch_acc;
drop snapshot if exists issue_27103_snapshot;
drop database if exists issue_27103_live_src;
drop database if exists issue_27103_live_dst;
drop database if exists issue_27103_snapshot_src;
drop database if exists issue_27103_snapshot_dst;
drop database if exists issue_27103_external_only_src;
drop database if exists issue_27103_external_only_dst;
-- @wait_expect(1, 30)
select count(*) as stale_objects from mo_catalog.mo_database
where datname in ('issue_27103_live_src', 'issue_27103_live_dst',
                  'issue_27103_snapshot_src', 'issue_27103_snapshot_dst',
                  'issue_27103_external_only_src', 'issue_27103_external_only_dst');
-- @wait_expect(1, 30)
select count(*) as stale_snapshots from mo_catalog.mo_snapshots
where sname = 'issue_27103_snapshot';
-- @wait_expect(1, 30)
select count(*) as stale_accounts from mo_catalog.mo_account
where account_name = 'issue_27103_branch_acc';

create database issue_27103_live_src;
create table issue_27103_live_src.control_t(id int primary key, payload varchar(20));
insert into issue_27103_live_src.control_t values (1, 'ordinary');
create external table issue_27103_live_src.ext_t(id int, payload varchar(32))
infile{"filepath"='$resources/external_table_file/aaa.csv','format'='csv'}
fields terminated by ',';
create function issue_27103_live_src.f_control() returns int language sql as
'select count(*) from issue_27103_live_src.control_t';
create function issue_27103_live_src.py_add(x int) returns int language python as
'return x + 1' handler 'py_add';
create function issue_27103_live_src.f_external() returns int language sql as
'select count(*) from issue_27103_live_src.ext_t';
create function issue_27103_live_src.f_transitive() returns int language sql as
'issue_27103_live_src.f_external()';
create view issue_27103_live_src.ext_v as select * from issue_27103_live_src.ext_t;
use issue_27103_live_src;
create view issue_27103_live_src.udf_v as select f_external();
create function issue_27103_live_src.f_view() returns int language sql as
'select count(*) from issue_27103_live_src.ext_v';
create procedure issue_27103_live_src.p_control() 'begin select count(*) as answer from issue_27103_live_src.control_t; end';
create procedure issue_27103_live_src.p_external() 'begin select count(*) as answer from issue_27103_live_src.ext_t; end';
create procedure issue_27103_live_src.p_transitive() 'begin call issue_27103_live_src.p_external(); end';
create procedure issue_27103_live_src.p_view() 'begin select count(*) as answer from issue_27103_live_src.ext_v; end';
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
select count(*) as live_target_omitted_udf_views from mo_catalog.mo_tables
where reldatabase = 'issue_27103_live_dst' and relkind = 'v' and relname = 'udf_v';
select count(*) as live_target_control_tables from mo_catalog.mo_tables
where reldatabase = 'issue_27103_live_dst' and relname = 'control_t' and relkind = 'r';
use issue_27103_live_dst;
select f_control() as live_independent_function_result;
call issue_27103_live_dst.p_control();
select count(*) as live_independent_functions from mo_catalog.mo_user_defined_function
where db = 'issue_27103_live_dst' and name = 'f_control';
select count(*) as live_opaque_python_functions from mo_catalog.mo_user_defined_function
where db = 'issue_27103_live_dst' and name = 'py_add';
select count(*) as live_external_functions from mo_catalog.mo_user_defined_function
where db = 'issue_27103_live_dst' and name in ('f_external', 'f_transitive', 'f_view');
select count(*) as live_independent_procedures from mo_catalog.mo_stored_procedure
where db = 'issue_27103_live_dst' and name = 'p_control';
select count(*) as live_external_procedures from mo_catalog.mo_stored_procedure
where db = 'issue_27103_live_dst' and name in ('p_external', 'p_transitive', 'p_view');
select count(*) as source_external_rows_after_clone from issue_27103_live_src.ext_t;

create database issue_27103_snapshot_src;
create table issue_27103_snapshot_src.control_t(id int primary key, payload varchar(20));
insert into issue_27103_snapshot_src.control_t values (2, 'snapshot');
create external table issue_27103_snapshot_src.ext_t(id int, payload varchar(32))
infile{"filepath"='$resources/external_table_file/aaa.csv','format'='csv'}
fields terminated by ',';
create function issue_27103_snapshot_src.f_control() returns int language sql as
'select count(*) from issue_27103_snapshot_src.control_t';
create function issue_27103_snapshot_src.py_add(x int) returns int language python as
'return x + 1' handler 'py_add';
create function issue_27103_snapshot_src.f_external() returns int language sql as
'select count(*) from issue_27103_snapshot_src.ext_t';
create function issue_27103_snapshot_src.f_transitive() returns int language sql as
'issue_27103_snapshot_src.f_external()';
create view issue_27103_snapshot_src.ext_v as select * from issue_27103_snapshot_src.ext_t;
use issue_27103_snapshot_src;
create view issue_27103_snapshot_src.udf_v as select f_external();
create function issue_27103_snapshot_src.f_view() returns int language sql as
'select count(*) from issue_27103_snapshot_src.ext_v';
create procedure issue_27103_snapshot_src.p_control() 'begin select count(*) as answer from issue_27103_snapshot_src.control_t; end';
create procedure issue_27103_snapshot_src.p_external() 'begin select count(*) as answer from issue_27103_snapshot_src.ext_t; end';
create procedure issue_27103_snapshot_src.p_transitive() 'begin call issue_27103_snapshot_src.p_external(); end';
create procedure issue_27103_snapshot_src.p_view() 'begin select count(*) as answer from issue_27103_snapshot_src.ext_v; end';
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
select count(*) as snapshot_target_omitted_udf_views from mo_catalog.mo_tables
where reldatabase = 'issue_27103_snapshot_dst' and relkind = 'v' and relname = 'udf_v';
select count(*) as snapshot_target_control_tables from mo_catalog.mo_tables
where reldatabase = 'issue_27103_snapshot_dst' and relname = 'control_t' and relkind = 'r';
use issue_27103_snapshot_dst;
select f_control() as snapshot_independent_function_result;
call issue_27103_snapshot_dst.p_control();
select count(*) as snapshot_independent_functions from mo_catalog.mo_user_defined_function
where db = 'issue_27103_snapshot_dst' and name = 'f_control';
select count(*) as snapshot_opaque_python_functions from mo_catalog.mo_user_defined_function
where db = 'issue_27103_snapshot_dst' and name = 'py_add';
select count(*) as snapshot_external_functions from mo_catalog.mo_user_defined_function
where db = 'issue_27103_snapshot_dst' and name in ('f_external', 'f_transitive', 'f_view');
select count(*) as snapshot_independent_procedures from mo_catalog.mo_stored_procedure
where db = 'issue_27103_snapshot_dst' and name = 'p_control';
select count(*) as snapshot_external_procedures from mo_catalog.mo_stored_procedure
where db = 'issue_27103_snapshot_dst' and name in ('p_external', 'p_transitive', 'p_view');

create database issue_27103_external_only_src;
create external table issue_27103_external_only_src.ext_t(id int, payload varchar(32))
infile{"filepath"='$resources/external_table_file/aaa.csv','format'='csv'}
fields terminated by ',';
create database issue_27103_external_only_dst clone issue_27103_external_only_src;
select count(*) as external_only_target_database from mo_catalog.mo_database
where datname = 'issue_27103_external_only_dst';
select count(*) as external_only_target_tables from mo_catalog.mo_tables
where reldatabase = 'issue_27103_external_only_dst';

-- DATA BRANCH uses the same cloneable-object set for source authorization.
-- The non-admin has SELECT only on the ordinary table; the external table is
-- deliberately not granted because it will not be materialized in the branch.
create account issue_27103_branch_acc admin_name = 'admin' identified by '111';
-- @session:id=11&user=issue_27103_branch_acc:admin&password=111
create database issue_27103_branch_src;
create table issue_27103_branch_src.control_t(id int primary key);
insert into issue_27103_branch_src.control_t values (11);
create external table issue_27103_branch_src.ext_t(id int, payload varchar(32))
infile{"filepath"='$resources/external_table_file/aaa.csv','format'='csv'}
fields terminated by ',';
create role issue_27103_branch_role;
grant connect on account * to issue_27103_branch_role;
grant create database on account * to issue_27103_branch_role;
create user issue_27103_branch_user identified by '111' default role issue_27103_branch_role;
grant select on table issue_27103_branch_src.control_t to issue_27103_branch_role;
-- @session:id=12&user=issue_27103_branch_acc:issue_27103_branch_user:issue_27103_branch_role&password=111
data branch create database issue_27103_branch_dst from issue_27103_branch_src;
-- @session:id=11
select count(*) as branch_target_database from mo_catalog.mo_database
where datname = 'issue_27103_branch_dst';
select count(*) as branch_target_control_tables from mo_catalog.mo_tables
where reldatabase = 'issue_27103_branch_dst' and relname = 'control_t' and relkind = 'r';
select count(*) as branch_target_external_tables from mo_catalog.mo_tables
where reldatabase = 'issue_27103_branch_dst' and relkind = 'e';
select * from issue_27103_branch_dst.control_t;
-- @session
drop account issue_27103_branch_acc;

drop snapshot if exists issue_27103_snapshot;
drop database if exists issue_27103_live_src;
drop database if exists issue_27103_live_dst;
drop database if exists issue_27103_snapshot_src;
drop database if exists issue_27103_snapshot_dst;
drop database if exists issue_27103_external_only_src;
drop database if exists issue_27103_external_only_dst;
