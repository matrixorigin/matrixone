-- Issue #26769: Power Query requires OFFSET without LIMIT for LimitOffset folding.
drop table if exists offset_without_limit;
create table offset_without_limit (id int primary key);
insert into offset_without_limit values (1), (2), (3);

select id from offset_without_limit order by id offset 1;
select id from offset_without_limit order by id offset 3;

prepare offset_without_limit_stmt from 'select id from offset_without_limit order by id offset ?';
set @skip_rows = 1;
execute offset_without_limit_stmt using @skip_rows;
set @skip_rows = 2;
execute offset_without_limit_stmt using @skip_rows;
deallocate prepare offset_without_limit_stmt;

-- 有序派生表与直接查询使用同一分页窗口。
select id from offset_without_limit order by id desc limit 1;
select * from (select id from offset_without_limit order by id desc) d limit 1;
select * from (select id from offset_without_limit order by id desc) d limit 1 offset 1;
select * from (select id from offset_without_limit order by id desc) d offset 2;
select * from (select id from offset_without_limit order by id desc limit 1 offset 1) d limit 3;
select * from (select id from offset_without_limit order by id desc limit 2 offset 1) d limit 3 offset 1;
select * from (select id from offset_without_limit order by id desc limit 1) d limit 3 offset 1;
select * from (select id from offset_without_limit order by id desc) d limit 0;

-- 不得越过过滤、聚合、窗口或不同的外层排序提前截断。
select * from (select id from offset_without_limit order by id desc limit 3) d where id < 3 limit 1;
select count(*) from (select id from offset_without_limit order by id desc) d limit 1;
select * from (select id from offset_without_limit order by id desc) d order by id limit 1;
select * from (select distinct id % 2 as parity from offset_without_limit order by parity desc) d limit 1;
-- @sortkey:0
select distinct parity from (select id % 2 as parity from offset_without_limit order by parity desc limit 3) d limit 2;
select * from (select id, sum(id) over () as total from offset_without_limit order by id desc) d limit 1;
select id, sum(id) over () as total from (select id from offset_without_limit order by id desc limit 3) d limit 1;

-- 预编译参数必须在每次执行重新绑定，不能固化第一次的分页边界。
prepare derived_sort_page from 'select * from (select id from offset_without_limit order by id desc) d limit ? offset ?';
set @page_size = 1;
set @page_offset = 0;
execute derived_sort_page using @page_size, @page_offset;
set @page_size = 2;
set @page_offset = 1;
execute derived_sort_page using @page_size, @page_offset;
deallocate prepare derived_sort_page;
prepare nested_sort_page from 'select * from (select id from offset_without_limit order by id desc limit ? offset 1) d limit ?';
set @inner_size = 1;
set @outer_size = 3;
execute nested_sort_page using @inner_size, @outer_size;
set @inner_size = 2;
execute nested_sort_page using @inner_size, @outer_size;
deallocate prepare nested_sort_page;

-- 多 step 递归查询的外层零需求必须同时省略 producer 与 consumer。
with recursive r(n) as (select 1 union all select n + 1 from r where n < 3)
select * from (select n from r order by n) d limit 0;
with recursive r(n) as (select 1 union all select n + 1 from r where n < 3)
select * from (select n from r order by n) d limit 0 offset 1;
with recursive r(n) as (select 1 union all select n + 1 from r where n < 3)
select * from (select n from r order by n limit 2 offset 1) d limit 0;
with recursive r(n) as (select 1 union all select n + 1 from r where n < 3)
select * from (select n from r order by n) d limit 2;

-- 零与非零参数交替执行，不能遗留上次执行的短路状态。
prepare recursive_sort_page from 'with recursive r(n) as (select 1 union all select n + 1 from r where n < 3) select * from (select n from r order by n) d limit ?';
set @recursive_page_size = 2;
execute recursive_sort_page using @recursive_page_size;
set @recursive_page_size = 0;
execute recursive_sort_page using @recursive_page_size;
set @recursive_page_size = 1;
execute recursive_sort_page using @recursive_page_size;
set @recursive_page_size = 0;
execute recursive_sort_page using @recursive_page_size;
deallocate prepare recursive_sort_page;

drop table offset_without_limit;
