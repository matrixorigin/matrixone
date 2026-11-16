-- View authorization keeps tenant-scoped roles and checks each base object.
drop snapshot if exists plan_view_sys_snap;
drop snapshot if exists plan_view_src_snap;
drop account if exists plan_view_src;
drop account if exists plan_view_dst;
drop database if exists plan_view_origin;
create account plan_view_src admin_name 'admin' identified by '111';
create account plan_view_dst admin_name 'admin' identified by '111';

-- A sys-owned view cloned into a different tenant retains legacy owner 0.
create database plan_view_origin;
create table plan_view_origin.t(a int);
insert into plan_view_origin.t values (1),(2);
create view plan_view_origin.v as select * from plan_view_origin.t;
create view plan_view_origin.const_v as select 3 as a;
create view plan_view_origin.nested_v as select * from plan_view_origin.v;
create snapshot plan_view_sys_snap for account;
create database plan_view_clone clone plan_view_origin {snapshot='plan_view_sys_snap'} to account plan_view_dst;

-- @session:id=1&user=plan_view_dst:admin&password=111
set enable_privilege_cache = on;
select * from plan_view_clone.v order by a;
select * from plan_view_clone.const_v;
select * from plan_view_clone.nested_v order by a;
set enable_privilege_cache = off;
select * from plan_view_clone.v order by a;
select * from plan_view_clone.const_v;
select * from plan_view_clone.nested_v order by a;
create role plan_view_reader;
grant connect on account * to plan_view_reader;
grant select on view plan_view_clone.v to plan_view_reader;
grant select on view plan_view_clone.const_v to plan_view_reader;
create user plan_view_user identified by '111' default role plan_view_reader;
-- @session

-- A view-only ordinary role must not turn an invalid legacy owner into admin.
-- @session:id=2&user=plan_view_dst:plan_view_user:plan_view_reader&password=111
select * from plan_view_clone.t;
select * from plan_view_clone.v;
select * from plan_view_clone.const_v;
-- @session

drop database plan_view_origin;
-- @session:id=1&user=plan_view_dst:admin&password=111
select * from plan_view_clone.v order by a;
select * from plan_view_clone.const_v;
select * from plan_view_clone.nested_v order by a;
-- @session

-- Local views over a subscription keep the subscriber's authorization context.
create database plan_view_pubdata;
create table plan_view_pubdata.t(a int);
insert into plan_view_pubdata.t values (30),(40);
create publication plan_view_pub database plan_view_pubdata account all;
-- @session:id=1&user=plan_view_dst:admin&password=111
create database plan_view_sub from sys publication plan_view_pub;
create database plan_view_subwrap;
create view plan_view_subwrap.definer_v as select * from plan_view_sub.t;
set view_security_type = 'INVOKER';
create view plan_view_subwrap.invoker_v as select * from plan_view_sub.t;
set view_security_type = 'DEFINER';
create role plan_sub_view_only;
grant connect on account * to plan_sub_view_only;
grant select on view plan_view_subwrap.definer_v to plan_sub_view_only;
grant select on view plan_view_subwrap.invoker_v to plan_sub_view_only;
create user plan_sub_view_user identified by '111' default role plan_sub_view_only;
create role plan_sub_table_reader;
grant connect on account * to plan_sub_table_reader;
grant select on view plan_view_subwrap.definer_v to plan_sub_table_reader;
grant select on view plan_view_subwrap.invoker_v to plan_sub_table_reader;
grant select on table plan_view_sub.* to plan_sub_table_reader;
create user plan_sub_table_user identified by '111' default role plan_sub_table_reader;
-- @session
-- @session:id=8&user=plan_view_dst:plan_sub_view_user:plan_sub_view_only&password=111
set enable_privilege_cache = on;
select * from plan_view_subwrap.definer_v order by a;
select * from plan_view_subwrap.invoker_v order by a;
set enable_privilege_cache = off;
select * from plan_view_subwrap.definer_v order by a;
select * from plan_view_subwrap.invoker_v order by a;
-- @session
-- @session:id=9&user=plan_view_dst:plan_sub_table_user:plan_sub_table_reader&password=111
select * from plan_view_subwrap.definer_v order by a;
select * from plan_view_subwrap.invoker_v order by a;
-- @session

-- @session:id=3&user=plan_view_src:admin&password=111
create database plan_view_data;
create table plan_view_data.t(a int);
insert into plan_view_data.t values (10),(20);
create role plan_view_definer;
grant connect on account * to plan_view_definer;
grant create view on database plan_view_data to plan_view_definer;
grant select on table plan_view_data.t to plan_view_definer;
create user plan_view_owner identified by '111' default role plan_view_definer;
-- @session

-- @session:id=4&user=plan_view_src:plan_view_owner:plan_view_definer&password=111
create view plan_view_data.v as select * from plan_view_data.t;
create view plan_view_data.const_v as select 12345 as a;
create view plan_view_data.nested_v as select * from plan_view_data.v;
-- @session

-- @session:id=3&user=plan_view_src:admin&password=111
create role plan_view_src_reader;
grant connect on account * to plan_view_src_reader;
grant select on view plan_view_data.v to plan_view_src_reader;
create user plan_view_src_user identified by '111' default role plan_view_src_reader;
-- @session

create snapshot plan_view_src_snap for account plan_view_src;
-- The sys caller's role and the view definer's role belong to different tenants.
set enable_privilege_cache = on;
select * from plan_view_data.v{snapshot='plan_view_src_snap'} order by a;
select * from plan_view_data.v{snapshot='plan_view_src_snap'} order by a;
select * from plan_view_data.nested_v{snapshot='plan_view_src_snap'} order by a;
select * from plan_view_data.const_v{snapshot='plan_view_src_snap'};
prepare plan_view_foreign_stmt from "select * from plan_view_data.v{snapshot='plan_view_src_snap'} order by a";
execute plan_view_foreign_stmt;
execute plan_view_foreign_stmt;

-- A sys DEFINER must not turn an ordinary caller into a cross-account reader.
create database plan_view_wrapper;
create table plan_view_wrapper.public_t(a int);
insert into plan_view_wrapper.public_t values (7);
create table plan_view_wrapper.sink(a int);
create view plan_view_wrapper.const_local1 as select 101 as a;
create view plan_view_wrapper.const_local2 as select 102 as a;
create view plan_view_wrapper.const_nested as select * from plan_view_wrapper.const_local1;
set view_security_type = 'INVOKER';
create view plan_view_wrapper.const_nested_invoker as select * from plan_view_wrapper.const_local2;
set view_security_type = 'DEFINER';
create view plan_view_wrapper.const_foreign_granted as select * from plan_view_data.const_v{snapshot='plan_view_src_snap'};
create view plan_view_wrapper.const_foreign_ungranted as select * from plan_view_data.const_v{snapshot='plan_view_src_snap'};
create view plan_view_wrapper.definer_v as select * from plan_view_data.v{snapshot='plan_view_src_snap'};
set view_security_type = 'INVOKER';
create view plan_view_wrapper.invoker_v as select * from plan_view_data.v{snapshot='plan_view_src_snap'};
set view_security_type = 'DEFINER';
create view plan_view_wrapper.ungranted_v as select * from plan_view_data.v{snapshot='plan_view_src_snap'};
create role plan_view_sys_reader;
grant connect on account * to plan_view_sys_reader;
grant select on view plan_view_wrapper.definer_v to plan_view_sys_reader;
grant select on view plan_view_wrapper.invoker_v to plan_view_sys_reader;
grant select on view plan_view_wrapper.const_local1 to plan_view_sys_reader;
grant select on view plan_view_wrapper.const_nested to plan_view_sys_reader;
grant select on view plan_view_wrapper.const_nested_invoker to plan_view_sys_reader;
grant select on view plan_view_wrapper.const_foreign_granted to plan_view_sys_reader;
grant select on table plan_view_wrapper.public_t to plan_view_sys_reader;
grant insert on table plan_view_wrapper.sink to plan_view_sys_reader;
grant create table on database plan_view_wrapper to plan_view_sys_reader;
create user plan_view_sys_user identified by '111' default role plan_view_sys_reader;
select * from plan_view_wrapper.definer_v order by a;
-- @session:id=6&user=sys:plan_view_sys_user:plan_view_sys_reader&password=111
select * from plan_view_data.v{snapshot='plan_view_src_snap'};
select * from plan_view_wrapper.definer_v;
select * from plan_view_wrapper.invoker_v;
select * from plan_view_wrapper.ungranted_v;
prepare plan_view_reader_stmt from 'select * from plan_view_wrapper.definer_v';
-- Constant views require their own grants even when another branch has a table.
select 1;
select * from plan_view_wrapper.const_local1;
select * from plan_view_wrapper.const_local2;
select * from plan_view_wrapper.const_nested;
select * from plan_view_wrapper.const_nested_invoker;
select * from plan_view_data.const_v{snapshot='plan_view_src_snap'};
select * from plan_view_wrapper.const_foreign_granted;
select * from plan_view_wrapper.const_foreign_ungranted;
select * from plan_view_wrapper.public_t join plan_view_wrapper.const_local2 on true;
select * from plan_view_wrapper.const_local1 union all select * from plan_view_wrapper.const_local2;
select * from plan_view_wrapper.const_local2 union all select * from plan_view_wrapper.const_local1;
set enable_privilege_cache = off;
select * from plan_view_wrapper.const_local2;
set enable_privilege_cache = on;
prepare plan_const_stmt from 'select * from plan_view_wrapper.const_local1';
execute plan_const_stmt;
insert into plan_view_wrapper.sink select * from plan_view_wrapper.const_local2;
create table plan_view_wrapper.ctas_denied as select * from plan_view_wrapper.const_local2;
-- @session
select count(*) from plan_view_wrapper.sink;
revoke select on view plan_view_wrapper.const_local1 from plan_view_sys_reader;
grant select on view plan_view_wrapper.const_local2 to plan_view_sys_reader;
-- @session:id=6&user=sys:plan_view_sys_user:plan_view_sys_reader&password=111
execute plan_const_stmt;
select * from plan_view_wrapper.const_nested_invoker;
insert into plan_view_wrapper.sink select * from plan_view_wrapper.const_local2;
create table plan_view_wrapper.ctas_ok as select * from plan_view_wrapper.const_local2;
-- @session
select count(*) from plan_view_wrapper.sink;
select * from plan_view_wrapper.ctas_ok;
revoke insert on table plan_view_wrapper.sink from plan_view_sys_reader;
grant select on view plan_view_wrapper.const_local1 to plan_view_sys_reader;
-- @session:id=6&user=sys:plan_view_sys_user:plan_view_sys_reader&password=111
execute plan_const_stmt;
deallocate prepare plan_const_stmt;
insert into plan_view_wrapper.sink select * from plan_view_wrapper.const_local2;
-- @session
select count(*) from plan_view_wrapper.sink;

-- A same-tenant reader is still governed by the view definer's grants.
-- @session:id=3&user=plan_view_src:admin&password=111
revoke select on table plan_view_data.t from plan_view_definer;
-- @session
-- @session:id=7&user=plan_view_src:plan_view_src_user:plan_view_src_reader&password=111
select * from plan_view_data.v order by a;
set enable_privilege_cache = off;
select * from plan_view_data.v order by a;
-- @session
-- The sys administrator can still read this other account's historical view.
select * from plan_view_data.v{snapshot='plan_view_src_snap'} order by a;
execute plan_view_foreign_stmt;
-- @session:id=3&user=plan_view_src:admin&password=111
grant select on table plan_view_data.t to plan_view_definer;
-- @session
-- @session:id=7&user=plan_view_src:plan_view_src_user:plan_view_src_reader&password=111
set enable_privilege_cache = on;
select * from plan_view_data.v order by a;
-- @session

-- Restore rebinds grants to new IDs; the sys snapshot reader retains access.
restore account plan_view_src{snapshot='plan_view_src_snap'};
select * from plan_view_data.v{snapshot='plan_view_src_snap'} order by a;
execute plan_view_foreign_stmt;
deallocate prepare plan_view_foreign_stmt;

-- A valid same-tenant definer still controls access even for an administrator.
create database plan_view_local;
create table plan_view_local.t(a int);
insert into plan_view_local.t values (7);
create role plan_view_local_definer;
grant connect on account * to plan_view_local_definer;
grant create view on database plan_view_local to plan_view_local_definer;
grant select on table plan_view_local.t to plan_view_local_definer;
create user plan_view_local_owner identified by '111' default role plan_view_local_definer;
-- @session:id=5&user=sys:plan_view_local_owner:plan_view_local_definer&password=111
create view plan_view_local.v as select * from plan_view_local.t;
create view plan_view_local.const_v as select 8 as a;
-- @session
revoke select on table plan_view_local.t from plan_view_local_definer;
select * from plan_view_local.t;
select * from plan_view_local.v;
-- A sys custom role cannot read an ungranted view.
create view plan_view_local.admin_v as select * from plan_view_local.t;
grant select on table plan_view_local.t to plan_view_sys_reader;
grant select on view plan_view_local.const_v to plan_view_sys_reader;
-- @session:id=6&user=sys:plan_view_sys_user:plan_view_sys_reader&password=111
select * from plan_view_local.t;
select * from plan_view_local.admin_v;
select * from plan_view_local.const_v;
-- @session

drop user plan_view_local_owner;
drop role plan_view_local_definer;
-- @session:id=6&user=sys:plan_view_sys_user:plan_view_sys_reader&password=111
select * from plan_view_local.const_v;
-- @session
drop database plan_view_local;
drop user plan_view_sys_user;
drop role plan_view_sys_reader;
drop database plan_view_wrapper;
drop snapshot plan_view_sys_snap;
drop snapshot plan_view_src_snap;
drop account plan_view_src;
drop account plan_view_dst;
drop publication plan_view_pub;
drop database plan_view_pubdata;
