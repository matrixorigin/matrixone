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

-- index access for an ordinary user goes through the base table
-- @session:id=1&user=sys:hit_user_bvt:hit_role_bvt&password=123456
select x,y from hit_db_bvt.hit_t where y = 20;

-- Existing view DDL must not turn an internal table into a readable view.
alter view hit_db_bvt.hit_view_bvt as select y from hit_db_bvt.hit_t;
-- @regex("do not have privilege to access the hidden internal table",true)
alter view hit_db_bvt.hit_view_bvt as select a from hit_db_bvt.`__mo_index_secondary_hit_probe`;
-- Observe the unchanged stored definition independently as its owner.
-- @session
select y from hit_db_bvt.hit_view_bvt order by y;
-- @session:id=1&user=sys:hit_user_bvt:hit_role_bvt&password=123456

-- Nested expressions and ordering clauses share the direct-access boundary.
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
select count(*) from hit_db_bvt.`__mo_index_secondary_hit_probe`;
prepare hatch_pl from 'select count(*) from hit_db_bvt.`__mo_index_unique_hit_probe`';
execute hatch_pl;
deallocate prepare hatch_pl;
insert into hit_db_bvt.`__mo_index_secondary_hit_probe` values (5, 6);
select count(*) from hit_db_bvt.`__mo_index_secondary_hit_probe`;

-- guarded again after the session variable is reset
set session allow_hidden_index_table_access = off;
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
