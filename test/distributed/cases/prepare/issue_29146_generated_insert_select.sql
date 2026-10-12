-- @suit
-- @case
-- @desc: Issue #29146: prepared INSERT ... SELECT keeps target integer vector types aligned with stored generated columns.
-- @label:bvt
drop database if exists issue_29146;
create database issue_29146;
use issue_29146;

create table src (id int primary key, v int);
insert into src values (1, 10), (2, 20), (3, 30);
create table dst (
    id int primary key,
    v int,
    g int generated always as (v + 1) stored,
    index iv(v)
);

prepare p from 'insert into dst(id, v) select id, v + ? from src where id <= ?';
set @delta = 7, @cut = 3;
execute p using @delta, @cut;
select id, v, g, v + 1 from dst order by id;

truncate table dst;
set @delta = -3, @cut = 2;
execute p using @delta, @cut;
select id, v, g, v + 1 from dst order by id;

truncate table dst;
begin;
set @delta = 5, @cut = 3;
execute p using @delta, @cut;
rollback;
select count(*) from dst;

deallocate prepare p;
drop database issue_29146;
