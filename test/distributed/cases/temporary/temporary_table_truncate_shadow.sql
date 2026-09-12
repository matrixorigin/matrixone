drop database if exists temp_shadow_truncate;
create database temp_shadow_truncate;
use temp_shadow_truncate;

create table shadow(id int primary key, k int, index idx_k(k));
insert into shadow values (100, 7), (101, 8);

-- Prepare while the permanent table is visible. Creating the temporary table
-- must invalidate and rebuild this plan before EXECUTE.
prepare truncate_shadow_before_temp from 'truncate table shadow';

create temporary table shadow(id int auto_increment primary key, k int, index idx_k(k));
insert into shadow(k) values (7), (8);

-- TRUNCATE must operate on the visible temporary table and leave the
-- same-named permanent table untouched.
truncate table shadow;
select count(*) as temp_rows_after_direct from shadow;
insert into shadow(k) values (9);
select id as auto_id_after_direct from shadow;

-- Preparing while the temporary table is visible must also target that table.
prepare truncate_shadow_after_temp from 'truncate table shadow';
select count(*) as temp_rows_after_prepare from shadow;
insert into shadow(k) values (10), (11);

execute truncate_shadow_before_temp;
select count(*) as temp_rows_after_execute from shadow;
insert into shadow(k) values (12);
select id as auto_id_after_prepared_before from shadow;
execute truncate_shadow_after_temp;
select count(*) as auto_rows_after_prepared from shadow;
insert into shadow(k) values (13);
select id as auto_id_after_prepared_visible from shadow;

-- A second session sees the permanent table and verifies every temporary
-- target left its rows intact.
-- @session:id=1{
use temp_shadow_truncate;
select count(*) as permanent_rows from shadow;
select min(id) as permanent_min_id from shadow;
-- @session

deallocate prepare truncate_shadow_before_temp;
deallocate prepare truncate_shadow_after_temp;
drop table shadow;
drop database temp_shadow_truncate;
