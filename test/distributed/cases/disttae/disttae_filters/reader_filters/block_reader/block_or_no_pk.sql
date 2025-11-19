drop database if exists block_or_npk;
create database block_or_npk;
use block_or_npk;

select enable_fault_injection();

-- no pk table - int
create table npk_int(a int, b int);
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'block_or_npk.npk_int');
insert into npk_int select g.result, g.result * 10 from generate_series(1, 8192) g;
select a from npk_int where a = 10 or a in (20, 30, 40) order by a;
select a from npk_int where a between 100 and 102 or a >= 8190 order by a;
select a from npk_int where a < 4 or a > 8189 order by a;
select a from npk_int where a <= 5 or a >= 8191 order by a;
select a from npk_int where (a = 11 and a = 12) or a = 13 order by a;
select a from npk_int where a < 4 or a between 256 and 258 or a in (500, 600) or a >= 8191 order by a;
drop table npk_int;

-- no pk table - varchar (non-prefix filters)
create table npk_varchar(a varchar(64), b int);
select add_fault_point('fj/cn/flush_small_objs',':::','echo',40,'block_or_npk.npk_varchar');
insert into npk_varchar select cast(g.result as varchar), g.result from generate_series(1, 8192) g;
select a from npk_varchar where a = '10' or a in ('20', '30', '40') order by cast(a as int);
select a from npk_varchar where a between '8000' and '8003' or a between '900' and '903' order by cast(a as int);
select a from npk_varchar where a in ('5', '15', '25') or a = '512' order by cast(a as int);
select a from npk_varchar where (a = '200' and a = '201') or a = '300' order by cast(a as int);
select a from npk_varchar where a between '2500' and '2502' or a between '6000' and '6001' or a in ('700', '800') or a between '950' and '952' order by cast(a as int);
drop table npk_varchar;

drop database block_or_npk;

select disable_fault_injection();
