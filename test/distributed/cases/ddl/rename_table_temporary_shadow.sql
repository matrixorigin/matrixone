-- Standalone RENAME uses persisted names, while ALTER uses temporary aliases (#31085).
drop database if exists rename_temp_shadow;
create database rename_temp_shadow;
use rename_temp_shadow;
create table src(x int);
insert into src values(11);
create temporary table src(y int);
insert into src values(22);
rename table src to dst;
select y from src;
select x from dst;
-- @session:id=1{
use rename_temp_shadow;
select x from dst;
select * from src;
-- @session}
-- A temporary-only source is absent from the RENAME namespace.
rename table src to missing;
select y from src;
-- A temporary-only destination does not occupy a persisted name.
create temporary table target(z int);
insert into target values(44);
rename table dst to target;
select z from target;
drop temporary table target;
select x from target;
-- A persisted destination still conflicts when shadowed.
create table occupied(x int);
insert into occupied values(55);
create temporary table occupied(y int);
insert into occupied values(66);
rename table target to occupied;
select x from target;
select y from occupied;
-- @session:id=1{
select x from occupied;
-- @session}
-- A later missing source rejects the whole statement before publication.
rename table target to moved, absent to another;
select x from target;
select * from moved;
-- Chained names stay in the persisted statement-local namespace.
create temporary table intermediate(y int);
insert into intermediate values(77);
rename table target to intermediate, intermediate to final_base;
select y from intermediate;
select x from final_base;
-- PREPARE before a shadow is created must still rename the persisted source.
prepare r from 'rename table final_base to prepared_base';
create temporary table final_base(y int);
insert into final_base values(88);
execute r;
deallocate prepare r;
select y from final_base;
select x from prepared_base;
-- Ordinary ALTER retains temporary-first behavior after success and failure.
alter table src add column extra int default 7;
select y, extra from src;
alter table src rename to altered_temp;
select y, extra from altered_temp;
drop temporary table altered_temp;
drop temporary table occupied;
drop temporary table intermediate;
drop temporary table final_base;
drop database rename_temp_shadow;
