-- @suite
-- @case
-- @desc: PAD_CHAR_TO_FULL_LENGTH restores fixed-width CHAR retrieval semantics.
-- @label:bvt

drop table if exists pad_char_to_full_length_t;
set @saved_sql_mode = @@sql_mode;
set sql_mode = '';

create table pad_char_to_full_length_t (
    c char(8),
    unicode_c char(4),
    v varchar(8),
    short_c char(4)
);
insert into pad_char_to_full_length_t values ('MO', '你好', 'MO', 'MO');

drop table if exists pad_char_set8;
drop table if exists pad_char_set4;
drop table if exists pad_char_set_v8;
drop table if exists pad_char_set_text;
drop table if exists pad_char_promoted;
drop table if exists pad_char_window;
create table pad_char_set8 (c char(8));
create table pad_char_set4 (c char(4));
create table pad_char_set_v8 (c varchar(8));
create table pad_char_set_text (c text);
create table pad_char_promoted (c char(8), v varchar(8));
create table pad_char_window (id int, c char(8), v varchar(8));
insert into pad_char_set8 values ('MO');
insert into pad_char_set4 values ('MO');
insert into pad_char_set_v8 values ('MO');
insert into pad_char_set_text values ('MO');
insert into pad_char_promoted values ('MO', null), (null, 'MO');
insert into pad_char_window values (1, 'MO', null), (2, null, 'MO'), (3, null, 'X');

select char_length(c), length(c), hex(c), concat('>', c, '<'),
       char_length(unicode_c), length(unicode_c), hex(unicode_c),
       char_length(v), length(v), hex(v)
from pad_char_to_full_length_t;
select cast(c as varchar(8)), length(cast(c as varchar(8))), hex(cast(c as varchar(8)))
from pad_char_to_full_length_t;
select count(*) from pad_char_to_full_length_t where length(c) = 2;
select count(*) from pad_char_to_full_length_t where c = 'MO';
select count(*) from pad_char_to_full_length_t where c >= 'MO' and c <= 'MO';
select count(*) from pad_char_to_full_length_t where c between 'MO' and 'MO';
select count(*) from pad_char_to_full_length_t where c in ('MO', 'XX');
select count(*) from pad_char_to_full_length_t as lhs
join pad_char_to_full_length_t as rhs on lhs.c = rhs.v;
select count(*) from pad_char_to_full_length_t as lhs
join pad_char_to_full_length_t as rhs on lhs.c = rhs.short_c;
select count(*) from (select c from pad_char_set8 union select c from pad_char_set4) u;
select count(*) from (select c from pad_char_set8 intersect select c from pad_char_set4) i;
select count(*) from (select c from pad_char_set8 minus select c from pad_char_set4) m;
select count(*) from (select c from pad_char_set8 union select c from pad_char_set_v8) u;
select count(*) from (select c from pad_char_set8 intersect select c from pad_char_set_v8) i;
select count(*) from (select c from pad_char_set8 minus select c from pad_char_set_v8) m;
select length(c), hex(c) from (select c from pad_char_set8 intersect select c from pad_char_set_v8) i;
select count(*) from (select c from pad_char_set_v8 union select c from pad_char_set8) u;
select count(*) from (select c from pad_char_set_v8 intersect select c from pad_char_set8) i;
select count(*) from (select c from pad_char_set_v8 minus select c from pad_char_set8) m;
select length(c), hex(c) from (select c from pad_char_set_v8 intersect select c from pad_char_set8) i;
select count(*) from (select c from pad_char_set8 union select c from pad_char_set_text) u;
select count(*) from (select c from pad_char_set8 intersect select c from pad_char_set_text) i;
select count(*) from (select c from pad_char_set8 minus select c from pad_char_set_text) m;
select count(*) from (select c from pad_char_set_text union select c from pad_char_set8) u;
select count(*) from (select c from pad_char_set_text intersect select c from pad_char_set8) i;
select count(*) from (select c from pad_char_set_text minus select c from pad_char_set8) m;
select count(*) from pad_char_promoted where coalesce(c, v) = 'MO';
select count(*) from (select distinct coalesce(c, v) from pad_char_promoted) d;
select count(*) from (select coalesce(c, v) from pad_char_promoted group by coalesce(c, v)) g;
select count(*) from (
    select coalesce(c, v) from pad_char_promoted
    union select c from pad_char_set_v8
) u;
select count(*) from (
    select coalesce(c, v) from pad_char_promoted
    intersect select c from pad_char_set_v8
) i;
select count(*) from (
    select coalesce(c, v) from pad_char_promoted
    minus select c from pad_char_set_v8
) m;
select count(*) from pad_char_promoted where if(c is null, v, c) = 'MO';
select count(*) from pad_char_promoted where case when c is null then v else c end = 'MO';
select count(*) from (
    select distinct x from (select coalesce(c, v) as x from pad_char_promoted) d
) q;
select count(*) from (
    select x from (select coalesce(c, v) as x from pad_char_promoted) d group by x
) q;
select count(*) from (
    select x from (select coalesce(c, v) as x from pad_char_promoted) d
    union select c from pad_char_set_v8
) q;
with d as (select coalesce(c, v) as x from pad_char_promoted)
select count(*) from (select distinct x from d) q;
select count(*) from (
    select distinct x from (
        select coalesce(c, v) as x from pad_char_promoted
        union all select c from pad_char_set_v8
    ) d
) q;
select count(*) from (select coalesce(c, v) as x from pad_char_promoted) d
where x in ('MO', 'XX');
select count(*) from (select coalesce(c, v) as x from pad_char_promoted) d
where x not in ('MO', 'XX');
select count(y) from (
    select distinct y from (
        select lag(coalesce(c, v)) over (order by id) as y from pad_char_window
    ) d
) q;

prepare pad_char_stmt from
    'select c, char_length(c), length(c), hex(c), concat(''>'', c, ''<''),
            cast(c as varchar(8)), length(cast(c as varchar(8))), hex(cast(c as varchar(8)))
     from pad_char_to_full_length_t';

set sql_mode = 'PAD_CHAR_TO_FULL_LENGTH';
select char_length(c), length(c), hex(c), concat('>', c, '<'),
       char_length(unicode_c), length(unicode_c), hex(unicode_c),
       char_length(v), length(v), hex(v)
from pad_char_to_full_length_t;
select cast(c as varchar(8)), length(cast(c as varchar(8))), hex(cast(c as varchar(8)))
from pad_char_to_full_length_t;
select count(*) from pad_char_to_full_length_t where length(c) = 8;
select count(*) from pad_char_to_full_length_t where c = 'MO';
select count(*) from pad_char_to_full_length_t where c >= 'MO' and c <= 'MO';
select count(*) from pad_char_to_full_length_t where c between 'MO' and 'MO';
select count(*) from pad_char_to_full_length_t where c in ('MO', 'XX');
select count(*) from pad_char_to_full_length_t as lhs
join pad_char_to_full_length_t as rhs on lhs.c = rhs.v;
select count(*) from pad_char_to_full_length_t as lhs
join pad_char_to_full_length_t as rhs on lhs.c = rhs.short_c;
select count(*) from (select c from pad_char_set8 union select c from pad_char_set4) u;
select count(*) from (select c from pad_char_set8 intersect select c from pad_char_set4) i;
select count(*) from (select c from pad_char_set8 minus select c from pad_char_set4) m;
select count(*) from (select c from pad_char_set8 union select c from pad_char_set_v8) u;
select count(*) from (select c from pad_char_set8 intersect select c from pad_char_set_v8) i;
select count(*) from (select c from pad_char_set8 minus select c from pad_char_set_v8) m;
select length(c), hex(c) from (select c from pad_char_set8 intersect select c from pad_char_set_v8) i;
select count(*) from (select c from pad_char_set_v8 union select c from pad_char_set8) u;
select count(*) from (select c from pad_char_set_v8 intersect select c from pad_char_set8) i;
select count(*) from (select c from pad_char_set_v8 minus select c from pad_char_set8) m;
select length(c), hex(c) from (select c from pad_char_set_v8 intersect select c from pad_char_set8) i;
select count(*) from (select c from pad_char_set8 union select c from pad_char_set_text) u;
select count(*) from (select c from pad_char_set8 intersect select c from pad_char_set_text) i;
select count(*) from (select c from pad_char_set8 minus select c from pad_char_set_text) m;
select count(*) from (select c from pad_char_set_text union select c from pad_char_set8) u;
select count(*) from (select c from pad_char_set_text intersect select c from pad_char_set8) i;
select count(*) from (select c from pad_char_set_text minus select c from pad_char_set8) m;
select count(*) from pad_char_promoted where coalesce(c, v) = 'MO';
select count(*) from (select distinct coalesce(c, v) from pad_char_promoted) d;
select count(*) from (select coalesce(c, v) from pad_char_promoted group by coalesce(c, v)) g;
select count(*) from (
    select coalesce(c, v) from pad_char_promoted
    union select c from pad_char_set_v8
) u;
select count(*) from (
    select coalesce(c, v) from pad_char_promoted
    intersect select c from pad_char_set_v8
) i;
select count(*) from (
    select coalesce(c, v) from pad_char_promoted
    minus select c from pad_char_set_v8
) m;
select count(*) from pad_char_promoted where if(c is null, v, c) = 'MO';
select count(*) from pad_char_promoted where case when c is null then v else c end = 'MO';
select count(*) from (
    select distinct x from (select coalesce(c, v) as x from pad_char_promoted) d
) q;
select count(*) from (
    select x from (select coalesce(c, v) as x from pad_char_promoted) d group by x
) q;
select count(*) from (
    select x from (select coalesce(c, v) as x from pad_char_promoted) d
    union select c from pad_char_set_v8
) q;
with d as (select coalesce(c, v) as x from pad_char_promoted)
select count(*) from (select distinct x from d) q;
select count(*) from (
    select distinct x from (
        select coalesce(c, v) as x from pad_char_promoted
        union all select c from pad_char_set_v8
    ) d
) q;
select count(*) from (select coalesce(c, v) as x from pad_char_promoted) d
where x in ('MO', 'XX');
select count(*) from (select coalesce(c, v) as x from pad_char_promoted) d
where x not in ('MO', 'XX');
select count(y) from (
    select distinct y from (
        select lag(coalesce(c, v)) over (order by id) as y from pad_char_window
    ) d
) q;
select length(c), hex(c) from (select c from pad_char_set8 union select c from pad_char_set4) u;
select length(c), hex(c) from (select c from pad_char_set4 union select c from pad_char_set8) u;
execute pad_char_stmt;

set sql_mode = '';
execute pad_char_stmt;

deallocate prepare pad_char_stmt;
set sql_mode = @saved_sql_mode;
drop table pad_char_to_full_length_t;
drop table pad_char_set8;
drop table pad_char_set4;
drop table pad_char_set_v8;
drop table pad_char_set_text;
drop table pad_char_promoted;
drop table pad_char_window;
