-- Restore into a different account must republish the account-scoped artifact.
drop snapshot if exists python_udf_revision_snapshot;
drop account if exists python_udf_restore_target;
drop account if exists python_udf_restore_source;
create account python_udf_restore_source admin_name = 'udf_admin' identified by '111';
create account python_udf_restore_target admin_name = 'udf_admin' identified by '111';
-- @session:id=1&user=python_udf_restore_source:udf_admin&password=111
create database udf_restore;
use udf_restore;
create function restored_python(x int) returns int language python as 'def restored_python(ctx,x): return x + 10' handler 'restored_python';
create function restored_sql(x int) returns int language sql as '$1 + 20';
create table expected_revisions as select f.name, f.function_id, f.active_revision, r.body from mo_catalog.mo_user_defined_function f join mo_catalog.mo_function_revisions r on f.function_id = r.function_id and f.active_revision = r.revision where f.db = 'udf_restore';
select restored_python(1) as python_value, restored_sql(1) as sql_value;
-- @session
create snapshot python_udf_revision_snapshot for account python_udf_restore_source;
-- @session:id=1&user=python_udf_restore_source:udf_admin&password=111
use udf_restore;
create or replace function restored_python(x int) returns int language python as 'def restored_python(ctx,x): return x + 100' handler 'restored_python';
select restored_python(1) as source_value;
-- @session
restore account python_udf_restore_source{snapshot="python_udf_revision_snapshot"} to account python_udf_restore_target;
-- @session:id=2&user=python_udf_restore_target:udf_admin&password=111
use udf_restore;
select restored_python(1) as python_value, restored_sql(1) as sql_value;
select count(*) as exact_restored_revisions from expected_revisions e join mo_catalog.mo_user_defined_function f on e.function_id = f.function_id and e.name = f.name and e.active_revision = f.active_revision join mo_catalog.mo_function_revisions r on f.function_id = r.function_id and f.active_revision = r.revision where f.db = 'udf_restore' and e.body = r.body;
-- @session:id=1&user=python_udf_restore_source:udf_admin&password=111
use udf_restore;
select restored_python(1) as source_unchanged;
-- @session
drop snapshot python_udf_revision_snapshot;
drop account python_udf_restore_target;
drop account python_udf_restore_source;
select count(*) as leftover_accounts from mo_catalog.mo_account where account_name in ('python_udf_restore_source','python_udf_restore_target');
select count(*) as leftover_snapshots from mo_catalog.mo_snapshots where sname = 'python_udf_revision_snapshot';
