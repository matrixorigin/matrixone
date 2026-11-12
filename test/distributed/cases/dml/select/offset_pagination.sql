drop table if exists offset_pagination;
create table offset_pagination (
    id int primary key,
    ts datetime not null,
    key i_ts (ts)
);
insert into offset_pagination values
    (7, '2026-01-01 00:00:00'),
    (2, '2026-01-01 00:00:00'),
    (8, '2026-01-02 00:00:00'),
    (3, '2026-01-02 00:00:00'),
    (5, '2026-01-01 00:00:00'),
    (1, '2026-01-01 00:00:00'),
    (6, '2026-01-02 00:00:00'),
    (4, '2026-01-02 00:00:00');

-- 单列结果用 sortkey 保留输出顺序，避免 runner 默认重排掩盖页内逆序。
-- 非唯一排序只约束时间戳顺序，不固定等键行的 ID 顺序。
-- @sortkey:0
select ts from offset_pagination order by ts limit 3;
-- @sortkey:0
select ts from offset_pagination order by ts limit 3 offset 3;

-- 唯一次级键定义全序；分页边界位于相同时间戳内部。
-- @sortkey:0
select id from offset_pagination order by ts, id;
-- @sortkey:0
select id from offset_pagination order by ts, id limit 3;
-- @sortkey:0
select id from offset_pagination order by ts, id limit 3 offset 3;
-- @sortkey:0
select id from offset_pagination order by ts, id limit 3 offset 6;
-- @sortkey:0
select id from offset_pagination order by ts, id limit 3 offset 9;

-- 倒序同样需要显式的唯一次级键。
-- @sortkey:0
select id from offset_pagination order by ts desc, id desc;
-- @sortkey:0
select id from offset_pagination order by ts desc, id desc limit 3;
-- @sortkey:0
select id from offset_pagination order by ts desc, id desc limit 3 offset 3;
-- @sortkey:0
select id from offset_pagination order by ts desc, id desc limit 3 offset 6;

-- 游标使用上一页末行的完整排序键，与第二页的 OFFSET 结果一致。
-- @sortkey:0
select id from offset_pagination
where ts > '2026-01-01 00:00:00'
   or (ts = '2026-01-01 00:00:00' and id > 5)
order by ts, id limit 3;
-- @sortkey:0
select id from offset_pagination
where ts < '2026-01-02 00:00:00'
   or (ts = '2026-01-02 00:00:00' and id < 4)
order by ts desc, id desc limit 3;

-- 同一预处理语句重新绑定不同窗口，再返回第一页。
prepare offset_pagination_stmt from 'select id from offset_pagination order by ts, id limit ? offset ?';
set @offset_pagination_limit = 3;
set @offset_pagination_skip = 0;
-- @sortkey:0
execute offset_pagination_stmt using @offset_pagination_limit, @offset_pagination_skip;
set @offset_pagination_skip = 3;
-- @sortkey:0
execute offset_pagination_stmt using @offset_pagination_limit, @offset_pagination_skip;
set @offset_pagination_skip = 0;
-- @sortkey:0
execute offset_pagination_stmt using @offset_pagination_limit, @offset_pagination_skip;
deallocate prepare offset_pagination_stmt;
set @offset_pagination_limit = null;
set @offset_pagination_skip = null;
drop table offset_pagination;
select count(*) as remaining_tables from information_schema.tables
where table_schema = database() and table_name = 'offset_pagination';
