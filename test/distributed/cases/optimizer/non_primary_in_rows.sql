-- 这里只比较落盘数据的查询结果；选择哪些块由 reader 的块集合测试断言。
drop database if exists non_primary_in_rows;
create database non_primary_in_rows;
use non_primary_in_rows;
create table src (id int primary key, k int, period varchar(16), amount int);
insert into src values (1,1,'2026.12',999),(2,0,'2024.12',10),(3,null,null,999),(5,100,'2024.12',20),(6,150,'2025.12',40);
-- @regex("returnStr.*OK",true)
select mo_ctl('dn','flush','non_primary_in_rows.src');

-- 相同域命中 5、6；不同域只有 6 同时命中。漏掉左、右任一谓词都会多出一行。
select id from src where k in (100,150,null) and k in (150,100,null) order by id;
select id from src where k in (100,150,null) and k in (150,1,null) order by id;
select id from src where period in ('2025.12',null,'2024.12','2025.12') order by id;
select count(*) from src where period in (null,null);
select count(*) from src where period not in ('2024.12',null);
prepare p from 'select id from src where period in (?,?) order by id';
set @a='2024.12';
set @b='2025.12';
execute p using @a,@b;
set @a=null;
execute p using @a,@b;
set @a='2024.12';
execute p using @a,@b;
deallocate prepare p;

-- 检查物化后的内容，不用执行计划代替行结果。
create table totals as with raw_data as (select period,sum(amount) as total from src where period in ('2024.12','2025.12') group by period) select period,total from raw_data;
select period,total from totals order by period;
create table diff as select k from src where k in (0,1,100,150) and k not in (1,100);
select k from diff order by k;
insert into diff select k from src where k in (0,150) and k not in (0);
select k from diff order by k;

-- 非单调的浮点 NaN 与可能截断的二进制上界，不能排除实际命中行。
create table bounds (id int primary key, f float, d double, b varbinary(40));
insert into bounds values (1,100,100,unhex(concat(repeat('ff',30),'01'))),(2,200,200,null);
-- @regex("returnStr.*OK",true)
select mo_ctl('dn','flush','non_primary_in_rows.bounds');
select id from bounds where f in (100,cast('NaN' as float)) order by id;
select id from bounds where f in (cast('NaN' as float),100) order by id;
select id from bounds where d in (100,cast('NaN' as double)) order by id;
select id from bounds where d in (cast('NaN' as double),100) order by id;
select id from bounds where b in (unhex('00'),unhex(concat(repeat('ff',30),'01'))) order by id;
prepare pf from 'select id from bounds where f in (?,?) order by id';
prepare pd from 'select id from bounds where d in (?,?) order by id';
set @hit=100;
set @other=150;
execute pf using @hit,@other;
execute pd using @hit,@other;
set @other=cast('NaN' as double);
execute pf using @hit,@other;
execute pd using @hit,@other;
set @other=150;
execute pf using @hit,@other;
execute pd using @hit,@other;
deallocate prepare pf;
deallocate prepare pd;
prepare pb from 'select id from bounds where b in (?,?) order by id';
set @miss=unhex('00');
set @key=unhex(concat(repeat('ff',30),'01'));
execute pb using @miss,@key;
set @key=null;
execute pb using @miss,@key;
set @key=unhex(concat(repeat('ff',30),'01'));
execute pb using @miss,@key;
deallocate prepare pb;
drop database non_primary_in_rows;
