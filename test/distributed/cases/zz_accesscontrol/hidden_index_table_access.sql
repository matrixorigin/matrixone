-- Direct access to hidden physical index tables (__mo_index_secondary_* /
-- __mo_index_unique_*) is rejected for ordinary users. Root keeps access.
-- The allow_hidden_index_table_access system variable is the debugging
-- escape hatch. mo_catalog.mo_indexes stays readable for name discovery.
drop role if exists hit_role_bvt;
drop user if exists hit_user_bvt;
drop database if exists hit_db_bvt;
create database hit_db_bvt;
use hit_db_bvt;
create table hit_t(x int primary key, y int, key idx_y(y));
insert into hit_t values (1,10),(2,20),(3,30);
create unique index uidx_y on hit_t(y);
create role hit_role_bvt;
grant select,insert,update,delete on table hit_db_bvt.* to hit_role_bvt;
grant alter view on database hit_db_bvt to hit_role_bvt;
-- CALL currently uses the create-view database privilege.
grant create view on database hit_db_bvt to hit_role_bvt;
grant connect on account * to hit_role_bvt;
create user hit_user_bvt identified by '123456' default role hit_role_bvt;
grant hit_role_bvt to hit_user_bvt;
select name,column_name from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where reldatabase = 'hit_db_bvt' and relname = 'hit_t') order by name, ordinal_position;
set @sit := (select index_table_name from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where reldatabase = 'hit_db_bvt' and relname = 'hit_t') and name = 'idx_y' and column_name = 'y');
set @sql := concat('select count(*) from `hit_db_bvt`.`', @sit, '`');

-- root may read the real hidden index table directly (system administration)
prepare root_sel from @sql;
execute root_sel;
deallocate prepare root_sel;

-- root also owns probe tables carrying the reserved prefix
create table hit_db_bvt.`__mo_index_secondary_hit_probe` (a int, b int);
insert into hit_db_bvt.`__mo_index_secondary_hit_probe` values (1,2),(3,4);
create table hit_db_bvt.`__mo_index_unique_hit_probe` (a int, b int);
insert into hit_db_bvt.`__mo_index_unique_hit_probe` values (1,2),(3,4);
select count(*) from hit_db_bvt.`__mo_index_secondary_hit_probe`;
create view hit_db_bvt.hit_view_bvt as select x from hit_db_bvt.hit_t;
create procedure hit_db_bvt.hit_read() 'begin select count(*) as hidden_rows from hit_db_bvt.__mo_index_secondary_hit_probe; end';
create procedure hit_db_bvt.hit_nested() 'begin call hit_db_bvt.hit_read(); end';
create procedure hit_db_bvt.hit_prepare() 'begin prepare hit_sp from "select count(*) from hit_db_bvt.__mo_index_secondary_hit_probe"; execute hit_sp; deallocate prepare hit_sp; end';
create procedure hit_db_bvt.hit_execute() 'begin execute hit_cached; end';
create procedure hit_db_bvt.hit_write() 'begin insert into hit_db_bvt.hit_t values (4,40); delete from hit_db_bvt.__mo_index_secondary_hit_probe; end';
create procedure hit_db_bvt.hit_cond() 'begin if (select count(*) from hit_db_bvt.__mo_index_secondary_hit_probe) > 0 then select 123; end if; end';
create procedure hit_db_bvt.hit_expr() 'begin declare n bigint default (select count(*) from hit_db_bvt.__mo_index_secondary_hit_probe); select n; end';
create procedure hit_db_bvt.hit_base() 'begin insert into hit_db_bvt.hit_t values (4,40); select x,y from hit_db_bvt.hit_t where y=40; delete from hit_db_bvt.hit_t where x=4; end';
create procedure hit_db_bvt.hit_dynamic(out status varchar) language 'starlark'
$$
_, setup_err = mo.sql("delete from hit_db_bvt.hit_t where x=-1")
rs, err = mo.sql("select count(*) from hit_db_bvt.__mo_index_secondary_hit_probe")
rc, rc_err = mo.sql("select row_count()")
rs2, err2 = mo.sql("select 1")
out_status = "denied_then_ok" if setup_err == None and err != None and "hidden internal table" in err and rc_err == None and rc[0][0] == "-1" and err2 == None else "unexpected"
$$;
-- The administrative exemption applies to stored SQL as well.
call hit_db_bvt.hit_nested();

-- index access for an ordinary user goes through the base table
-- @session:id=1&user=sys:hit_user_bvt:hit_role_bvt&password=123456
use hit_db_bvt;
select x,y from hit_db_bvt.hit_t where y = 20;

-- Stored SQL retains its caller's policy, including nested/dynamic SQL and
-- expressions evaluated outside the ordinary statement executor.
-- @regex("do not have privilege to access the hidden internal table",true)
call hit_db_bvt.hit_read();
-- @regex("do not have privilege to access the hidden internal table",true)
call hit_db_bvt.hit_nested();
-- @regex("do not have privilege to access the hidden internal table",true)
call hit_db_bvt.hit_prepare();
-- @regex("do not have privilege to access the hidden internal table",true)
call hit_db_bvt.hit_write();
-- A late denial rolls back earlier base-table work in the same CALL.
select x,y from hit_db_bvt.hit_t order by x;
-- @regex("do not have privilege to access the hidden internal table",true)
call hit_db_bvt.hit_cond();
-- @regex("do not have privilege to access the hidden internal table",true)
call hit_db_bvt.hit_expr();
call hit_db_bvt.hit_dynamic(@hit_status);
select @hit_status as stored_sql_status;
-- Normal procedure DML still maintains its secondary and unique indexes.
call hit_db_bvt.hit_base();
select x,y from hit_db_bvt.hit_t order by x;

-- Existing view DDL must not turn an internal table into a readable view.
alter view hit_db_bvt.hit_view_bvt as select y from hit_db_bvt.hit_t;
-- @regex("do not have privilege to access the hidden internal table",true)
alter view hit_db_bvt.hit_view_bvt as select a from hit_db_bvt.`__mo_index_secondary_hit_probe`;
-- Observe the unchanged stored definition independently as its owner.
-- @session
select y from hit_db_bvt.hit_view_bvt order by y;
-- @session:id=1&user=sys:hit_user_bvt:hit_role_bvt&password=123456

-- Nested expressions and ordering clauses share the direct-access boundary.
-- COLLATE and every sampled column must preserve the same caller policy.
-- @regex("do not have privilege to access the hidden internal table",true)
select cast((select count(*) from hit_db_bvt.__mo_index_secondary_hit_probe) as char) collate utf8mb4_bin;
-- @regex("do not have privilege to access the hidden internal table",true)
select sample((select count(*) from hit_db_bvt.__mo_index_secondary_hit_probe), 1 rows) from hit_db_bvt.hit_t;
-- @regex("do not have privilege to access the hidden internal table",true)
select sample(x, (select count(*) from hit_db_bvt.__mo_index_secondary_hit_probe), 100 percent) from hit_db_bvt.hit_t;
select cast((select count(*) from hit_db_bvt.hit_t) as char) collate utf8mb4_bin as base_count;
select sample(y, 100 percent) as sample_y from hit_db_bvt.hit_t order by sample_y;
-- @regex("do not have privilege to access the hidden internal table",true)
select serial_extract((select serial(count(*)) from hit_db_bvt.`__mo_index_secondary_hit_probe`), 0 as bigint);
-- @regex("do not have privilege to access the hidden internal table",true)
select percentile_disc(0.5) within group (order by (select count(*) from hit_db_bvt.`__mo_index_secondary_hit_probe`)) from hit_db_bvt.hit_t;
-- @regex("do not have privilege to access the hidden internal table",true)
select row_number() over (order by (select count(*) from hit_db_bvt.`__mo_index_secondary_hit_probe`)) from hit_db_bvt.hit_t;
-- @regex("do not have privilege to access the hidden internal table",true)
select row_number() over w from hit_db_bvt.hit_t window w as (order by (select count(*) from hit_db_bvt.`__mo_index_secondary_hit_probe`));
-- @regex("do not have privilege to access the hidden internal table",true)
explain phyplan select * from hit_db_bvt.`__mo_index_secondary_hit_probe`;
select percentile_disc(0.5) within group (order by y) as median_y from hit_db_bvt.hit_t;
select row_number() over (order by y) as rn from hit_db_bvt.hit_t order by rn;

-- every direct statement form against a hidden index table is rejected
-- @regex("do not have privilege to access the hidden internal table",true)
select count(*) from hit_db_bvt.`__mo_index_secondary_hit_probe`;
-- @regex("do not have privilege to access the hidden internal table",true)
show create table hit_db_bvt.`__mo_index_secondary_hit_probe`;
-- @regex("do not have privilege to access the hidden internal table",true)
show columns from hit_db_bvt.`__mo_index_secondary_hit_probe`;
-- SHOW table operands and user-written filters are not internal maintenance.
-- @regex("do not have privilege to access the hidden internal table",true)
show table_values from hit_db_bvt.__mo_index_secondary_hit_probe;
-- @regex("do not have privilege to access the hidden internal table",true)
show column_number from hit_db_bvt.__mo_index_secondary_hit_probe;
-- @regex("do not have privilege to access the hidden internal table",true)
show tables from hit_db_bvt where (select count(*) from hit_db_bvt.__mo_index_secondary_hit_probe) > 0;
-- @regex("do not have privilege to access the hidden internal table",true)
show columns from hit_db_bvt.hit_t where (select count(*) from hit_db_bvt.__mo_index_secondary_hit_probe) > 0;
-- @regex("do not have privilege to access the hidden internal table",true)
show index from hit_db_bvt.hit_t where (select count(*) from hit_db_bvt.__mo_index_secondary_hit_probe) > 0;
-- @regex("do not have privilege to access the hidden internal table",true)
show databases where (select count(*) from hit_db_bvt.__mo_index_secondary_hit_probe) > 0;
-- @regex("do not have privilege to access the hidden internal table",true)
show table status from hit_db_bvt where (select count(*) from hit_db_bvt.__mo_index_secondary_hit_probe) > 0;
-- @regex("do not have privilege to access the hidden internal table",true)
show procedure status where (select count(*) from hit_db_bvt.__mo_index_secondary_hit_probe) > 0;
-- @regex("do not have privilege to access the hidden internal table",true)
show triggers from hit_db_bvt where (select count(*) from hit_db_bvt.__mo_index_secondary_hit_probe) > 0;
show column_number from hit_db_bvt.hit_t;
-- @regex("do not have privilege to access the hidden internal table",true)
insert into hit_db_bvt.`__mo_index_secondary_hit_probe` values (5, 6);
-- @regex("do not have privilege to access the hidden internal table",true)
update hit_db_bvt.`__mo_index_secondary_hit_probe` set b = 9 where a = 1;
-- @regex("do not have privilege to access the hidden internal table",true)
delete from hit_db_bvt.`__mo_index_secondary_hit_probe`;
-- @regex("do not have privilege to access the hidden internal table",true)
select count(*) from hit_db_bvt.`__mo_index_unique_hit_probe`;
-- @regex("do not have privilege to access the hidden internal table",true)
insert into hit_db_bvt.`__mo_index_unique_hit_probe` values (5, 6);
-- @regex("do not have privilege to access the hidden internal table",true)
analyze table hit_db_bvt.`__mo_index_secondary_hit_probe`(a);
-- @regex("do not have privilege to access the hidden internal table",true)
drop table hit_db_bvt.`__mo_index_secondary_hit_probe`;
-- @regex("do not have privilege to access the hidden internal table",true)
truncate table hit_db_bvt.`__mo_index_secondary_hit_probe`;

-- prepared statements are guarded too
-- @regex("do not have privilege to access the hidden internal table",true)
prepare deny_pl from 'delete from hit_db_bvt.`__mo_index_unique_hit_probe` limit 1';
execute deny_pl;
deallocate prepare deny_pl;

-- the escape hatch lifts the guard for debugging (session scope)
set session allow_hidden_index_table_access = on;
show session variables like 'allow_hidden_index_table_access';
call hit_db_bvt.hit_nested();
call hit_db_bvt.hit_prepare();
prepare hit_cached from 'select count(*) from hit_db_bvt.__mo_index_secondary_hit_probe';
call hit_db_bvt.hit_execute();
select count(*) from hit_db_bvt.`__mo_index_secondary_hit_probe`;
prepare hatch_pl from 'select count(*) from hit_db_bvt.`__mo_index_unique_hit_probe`';
execute hatch_pl;
deallocate prepare hatch_pl;
insert into hit_db_bvt.`__mo_index_secondary_hit_probe` values (5, 6);
select count(*) from hit_db_bvt.`__mo_index_secondary_hit_probe`;

-- guarded again after the session variable is reset
set session allow_hidden_index_table_access = off;
-- @regex("do not have privilege to access the hidden internal table",true)
call hit_db_bvt.hit_nested();
-- Recheck handles prepared earlier with diagnostic access enabled.
-- @regex("do not have privilege to access the hidden internal table",true)
call hit_db_bvt.hit_execute();
deallocate prepare hit_cached;
-- @regex("do not have privilege to access the hidden internal table",true)
select count(*) from hit_db_bvt.`__mo_index_secondary_hit_probe`;
-- @session

-- index build/drop maintenance stays intact
create index idx_y2 on hit_db_bvt.hit_t(y);
select name from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where reldatabase = 'hit_db_bvt' and relname = 'hit_t') and ordinal_position = 1 order by name;
drop index idx_y2 on hit_db_bvt.hit_t;
select name from mo_catalog.mo_indexes where table_id = (select rel_id from mo_catalog.mo_tables where reldatabase = 'hit_db_bvt' and relname = 'hit_t') and ordinal_position = 1 order by name;

-- cleanup
set global allow_hidden_index_table_access = off;
drop table if exists hit_db_bvt.`__mo_index_secondary_hit_probe`;
drop table if exists hit_db_bvt.`__mo_index_unique_hit_probe`;
drop user if exists hit_user_bvt;
drop role if exists hit_role_bvt;
drop database if exists hit_db_bvt;
