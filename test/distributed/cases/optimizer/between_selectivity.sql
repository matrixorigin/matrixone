drop database if exists between_selectivity;
create database between_selectivity;
use between_selectivity;
create table ranges(id int primary key, v int, d decimal(18,2), wide decimal(30,3));
insert into ranges values (1,0,0,0), (2,19,19,19), (3,81,81,81), (4,100,100,100);
-- @regex("returnStr.*OK",true)
select mo_ctl('dn', 'flush', 'between_selectivity.ranges');
-- 采样块数与字节数不是本用例契约，保留行数、覆盖方式和完成状态的比较。
-- @ignore:7,8
analyze table ranges(v,d,wide) fullscan;

-- 空交集不能按查询区间宽度估算；实际结果是独立的语义对照。
-- @regex("Table Scan.*selectivity=0[.]0000.*",true)
explain verbose select id from ranges where v between -50 and -10;
select count(*) as matches from ranges where v between -50 and -10;
-- @regex("Table Scan.*selectivity=0[.]0000.*",true)
explain verbose select id from ranges where d between -50.00 and -10.00;
select count(*) as matches from ranges where d between -50.00 and -10.00;
-- @regex("Table Scan.*selectivity=0[.]0000.*",true)
explain verbose select id from ranges where wide between -50.000 and -10.000;
select count(*) as matches from ranges where wide between -50.000 and -10.000;

-- 部分重叠只估算交集；全覆盖应为 1，而不是异常回退值。
-- @regex("Table Scan.*selectivity=0[.]2000.*",true)
explain verbose select id from ranges where v between -20 and 19;
select id from ranges where v between -20 and 19 order by id;
-- @regex("Table Scan.*selectivity=1[.]0000.*",true)
explain verbose select id from ranges where v between -20 and 120;
select count(*) as matches from ranges where v between -20 and 120;
-- @regex("Table Scan.*selectivity=0[.]2000.*",true)
explain verbose select id from ranges where d between -20.00 and 19.00;
select id from ranges where d between -20.00 and 19.00 order by id;
-- @regex("Table Scan.*selectivity=1[.]0000.*",true)
explain verbose select id from ranges where wide between -20.000 and 120.000;
select count(*) as matches from ranges where wide between -20.000 and 120.000;

drop database between_selectivity;
