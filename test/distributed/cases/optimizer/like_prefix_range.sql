-- Literal prefix access uses analyzed candidate frequency, never NDV or a
-- guessed LIKE selectivity. Duplicate names match the reported distribution.
drop database if exists like_prefix_range;
create database like_prefix_range;
use like_prefix_range;
create table names(id int primary key, name varchar(20), payload int, key ix(name));
insert into names
select result, case result%10 when 0 then 'apple' when 1 then 'apply' when 2 then 'apricot' when 3 then 'banana' when 4 then 'band' when 5 then 'bank' when 6 then 'zebra' when 7 then 'zoo' when 8 then 'orange' else 'pear' end, result*2 from generate_series(0,499) g;
-- @regex("Index Table Scan",false)
explain select count(*) from names where name like 'ap%';
analyze table names(name) fullscan;
-- @regex("Index Table Scan",true)
explain select count(*) from names where name like 'ap%';
select count(*) as matches, sum(id) as ids from names where name like 'ap%';
select count(*) as matches, sum(id) as ids from names ignore index(ix)
where concat(name,'') like 'ap%';
select count(*) as mixed_matches from names where name like 'app_%';
select count(*) as suffix_matches from names where name like 'ap%y';
-- Existing equality covering access must not be displaced by the new prefix.
-- @regex("Join Type: INDEX",false)
-- @regex("Index Table Scan",true)
explain select name from names where name like 'ap%' and name='apple';
-- A selective existing IN competes with the LIKE prefix for backfill.
-- @regex("Join Type: INDEX",true)
-- @regex("prefix_in[(]",true)
explain select sum(payload) from names where name like 'ap%' and name in ('apple','apply');
select sum(payload) as payloads from names where name like 'ap%' and name in ('apple','apply');
-- @regex("Join Type: INDEX",true)
-- @regex("prefix_in[(]",true)
explain select sum(payload) from names where name in ('apple','apply') and name like 'ap%';
select sum(payload) as payloads from names ignore index(ix) where name in ('apple','apply') and concat(name,'') like 'ap%';
-- A broad observed prefix stays on the scan path after the next ANALYZE.
update names set name=concat('ap',id);
analyze table names(name) fullscan;
-- @regex("Index Table Scan",false)
explain select count(*) from names where name like 'ap%';
select count(*) as broad_matches from names where name like 'ap%';
drop database like_prefix_range;
