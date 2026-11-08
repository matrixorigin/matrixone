-- @label:bvt
-- Deep pagination must agree with the full-sort path for ties, filters,
-- nullable keys and repeated prepared executions.
drop database if exists ordered_pagination_cutoff;
create database ordered_pagination_cutoff;
use ordered_pagination_cutoff;
set @saved_optimizer_hints = @@optimizer_hints;
set @saved_max_dop = @@max_dop;
set @saved_query_max_workers = @@query_max_workers;
create table pages (id int primary key, k int not null, nullable_k int, b varbinary(64) not null,
    d decimal(12,2) not null, dt datetime not null);
insert into pages select result, (result-1) div 4, if(result%17=0, null, result),
    concat(lpad(result, 8, '0'), repeat('x', 40)), cast(result as decimal(12,2)),
    date_add('2020-01-01 00:00:00', interval result second) from generate_series(1, 20000) g;
select mo_ctl('dn', 'flush', 'ordered_pagination_cutoff.pages');
insert into pages select result, (result-1) div 4, if(result%17=0, null, result),
    concat(lpad(result, 8, '0'), repeat('x', 40)), cast(result as decimal(12,2)),
    date_add('2020-01-01 00:00:00', interval result second) from generate_series(20001, 40000) g;
select mo_ctl('dn', 'flush', 'ordered_pagination_cutoff.pages');

set session optimizer_hints = 'execType=2';
-- Limit both CN workers and per-CN readers for the pruning comparison.
set session query_max_workers = 1;
set max_dop = 1;
-- @regex("Sort[^\r\n]*\r?\n[^\r\n]*Analyze:[^\r\n]*\binputRows=[1-3][0-9]{4}\b",true)
explain analyze select id from pages order by id limit 5 offset 17000;
select id from pages order by id limit 5 offset 17000;
set max_dop = @saved_max_dop;
set session query_max_workers = @saved_query_max_workers;
select id, k from pages order by k, id desc limit 5 offset 17000;
select id, k from pages order by k desc, id limit 5 offset 17000;
select id from pages where k < 8000 order by id desc limit 5 offset 17000;
select id from pages order by b desc limit 5 offset 17000;
select id from pages order by d desc limit 5 offset 17000;
select id from pages order by dt limit 5 offset 17000;
select id from pages where id % 2 = 0 order by id limit 5 offset 17000;
select id, nullable_k from pages order by nullable_k, id desc limit 5 offset 17000;
select id from pages order by id limit 5 offset 50000;
select sql_calc_found_rows id from pages order by id limit 3 offset 17000;
select found_rows();

set session optimizer_hints = 'execType=2,sendMessageFromTopToScan=1';
set session query_max_workers = 1;
set max_dop = 1;
-- @regex("Sort[^\r\n]*\r?\n[^\r\n]*Analyze:[^\r\n]*\binputRows=40000\b",true)
explain analyze select id from pages order by id limit 5 offset 17000;
select id from pages order by id limit 5 offset 17000;
set max_dop = @saved_max_dop;
set session query_max_workers = @saved_query_max_workers;
select id, k from pages order by k, id desc limit 5 offset 17000;
select id, k from pages order by k desc, id limit 5 offset 17000;
select id from pages where k < 8000 order by id desc limit 5 offset 17000;
select id from pages order by b desc limit 5 offset 17000;
select id from pages order by d desc limit 5 offset 17000;
select id from pages order by dt limit 5 offset 17000;
select id from pages where id % 2 = 0 order by id limit 5 offset 17000;
select id, nullable_k from pages order by nullable_k, id desc limit 5 offset 17000;
select id from pages order by id limit 5 offset 50000;
select sql_calc_found_rows id from pages order by id limit 3 offset 17000;
select found_rows();

-- The same parameter types with different values exercise prepared-plan reuse.
set session optimizer_hints = 'execType=1';
prepare page_stmt from 'select id from pages order by id limit ? offset ?';
set @lim = 3;
set @off = 17000;
execute page_stmt using @lim, @off;
set @off = 25000;
execute page_stmt using @lim, @off;
set @off = 17000;
execute page_stmt using @lim, @off;
deallocate prepare page_stmt;
set session optimizer_hints = 'execType=2';
prepare page_stmt from 'select id from pages order by id limit ? offset ?';
set @off = 17000;
execute page_stmt using @lim, @off;
set @off = 25000;
execute page_stmt using @lim, @off;
set @off = 17000;
execute page_stmt using @lim, @off;
deallocate prepare page_stmt;
-- Count only visible rows when forming the cutoff, including workspace deletes.
set session optimizer_hints = 'execType=2';
set session query_max_workers = 1;
set max_dop = 1;
delete from pages where id <= 1000;
-- @regex("Sort[^\r\n]*\r?\n[^\r\n]*Analyze:[^\r\n]*\binputRows=(?:[12][0-9]{4}|3[0-8][0-9]{3})\b",true)
explain analyze select id from pages order by id limit 5 offset 17000;
select id from pages order by id limit 5 offset 17000;
begin;
delete from pages where id between 1001 and 2000;
-- @regex("Sort[^\r\n]*\r?\n[^\r\n]*Analyze:[^\r\n]*\binputRows=(?:[12][0-9]{4}|3[0-7][0-9]{3})\b",true)
explain analyze select id from pages order by id limit 5 offset 17000;
select id from pages order by id limit 5 offset 17000;
set session optimizer_hints = 'execType=2,sendMessageFromTopToScan=1';
-- @regex("Sort[^\r\n]*\r?\n[^\r\n]*Analyze:[^\r\n]*\binputRows=38000\b",true)
explain analyze select id from pages order by id limit 5 offset 17000;
select id from pages order by id limit 5 offset 17000;
rollback;
-- @regex("Sort[^\r\n]*\r?\n[^\r\n]*Analyze:[^\r\n]*\binputRows=39000\b",true)
explain analyze select id from pages order by id limit 5 offset 17000;
select id from pages order by id limit 5 offset 17000;
set max_dop = @saved_max_dop;
set session query_max_workers = @saved_query_max_workers;
set session optimizer_hints = @saved_optimizer_hints;
drop database ordered_pagination_cutoff;
