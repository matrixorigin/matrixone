drop database if exists block_or_cpk;
create database block_or_cpk;
use block_or_cpk;

select enable_fault_injection();

-- composite pk (int, int) - value range filters
create table cpk_int(a int, b int, c int, primary key(a, b));
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'block_or_cpk.cpk_int');
insert into cpk_int select g.result, g.result * 2, g.result * 3 from generate_series(1, 8192) g;
select a, b from cpk_int where a = 10 or a in (20, 30, 40) order by a, b;
select a, b from cpk_int where a between 100 and 102 or a >= 8190 order by a, b;
select a, b from cpk_int where a < 4 or a > 8189 order by a, b;
select a, b from cpk_int where a <= 5 or a >= 8191 order by a, b;
select a, b from cpk_int where (a = 11 and a = 12) or a = 13 order by a, b;
select a, b from cpk_int where a < 4 or a between 256 and 258 or a in (500, 600) or a >= 8191 order by a, b;
drop table cpk_int;

-- composite pk (varchar, varchar) - simple comparisons
create table cpk_varchar(a varchar(64), b varchar(64), c int, primary key(a, b));
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'block_or_cpk.cpk_varchar');
insert into cpk_varchar select cast(g.result as varchar), cast(g.result * 2 as varchar), g.result * 3 from generate_series(1, 8192) g;
select a, b from cpk_varchar where a = '10' or a in ('20', '30', '40') order by cast(a as int), cast(b as int);
select a, b from cpk_varchar where a between '8000' and '8003' or a between '900' and '903' order by cast(a as int), cast(b as int);
select a, b from cpk_varchar where a in ('5', '15', '25') or a = '512' order by cast(a as int), cast(b as int);
select a, b from cpk_varchar where (a = '200' and a = '201') or a = '300' order by cast(a as int), cast(b as int);
select a, b from cpk_varchar where a between '2500' and '2502' or a between '6000' and '6001' or a in ('700', '800') or a between '950' and '952' order by cast(a as int), cast(b as int);
drop table cpk_varchar;

drop database block_or_cpk;

select disable_fault_injection();
