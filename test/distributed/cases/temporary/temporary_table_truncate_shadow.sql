drop database if exists temp_shadow_truncate;
create database temp_shadow_truncate;
use temp_shadow_truncate;

create table shadow(id int primary key, k int, index idx_k(k));
insert into shadow values (100, 7), (101, 8);

-- Prepare while the permanent table is visible. Creating the temporary table
-- must invalidate and rebuild this plan before EXECUTE.
prepare truncate_shadow_before_temp from 'truncate table shadow';

create temporary table shadow(id int primary key, k int, index idx_k(k));
insert into shadow values (1, 7), (2, 8);

-- TRUNCATE on a temporary table is unsupported. Neither direct nor prepared
-- execution may fall through to the hidden permanent table.
truncate table shadow;
select count(*) as temp_rows_after_direct from shadow;

-- Preparing while the temporary table is visible must reject that table, not
-- resolve the hidden permanent one.
prepare truncate_shadow_after_temp from 'truncate table shadow';
select count(*) as temp_rows_after_prepare from shadow;

execute truncate_shadow_before_temp;
select count(*) as temp_rows_after_execute from shadow;

-- A second session sees the permanent table and verifies all rejected
-- paths left its rows intact.
-- @session:id=1{
use temp_shadow_truncate;
select count(*) as permanent_rows from shadow;
-- @session

deallocate prepare truncate_shadow_before_temp;
drop table shadow;
drop database temp_shadow_truncate;
