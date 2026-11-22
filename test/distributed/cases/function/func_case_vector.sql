-- @suit
-- @case
-- @desc:CASE, NULLIF, and IFNULL preserve vector result types and dimensions
-- @label:bvt

select case when true then cast('[1,2]' as vecf32(2)) else cast('[3,4]' as vecf32(2)) end;
select case when true then cast('[1,2]' as vecf64(2)) else cast('[3,4]' as vecf64(2)) end;
select case when true then cast('[1,2]' as vecf16(2)) else cast('[3,4]' as vecf16(2)) end;
select case when true then cast('[1,2]' as vecbf16(2)) else cast('[3,4]' as vecbf16(2)) end;
select case when true then cast('[1,2]' as vecint8(2)) else cast('[3,4]' as vecint8(2)) end;
select case when true then cast('[1,2]' as vecuint8(2)) else cast('[3,4]' as vecuint8(2)) end;

select case when false then cast('[1,2]' as vecf32(2)) end;
select case when true then cast(null as vecf32(2)) else cast('[3,4]' as vecf32(2)) end;
select ifnull(cast(null as vecf32(2)), cast('[1,2]' as vecf32(2)));
select nullif(cast('[1,2]' as vecf32(2)), cast('[1,2]' as vecf32(2)));
select nullif(cast('[1,2]' as vecf32(2)), cast('[3,4]' as vecf32(2)));
select ifnull(cast(null as vecf64(2)), cast('[1,2]' as vecf64(2))),
       nullif(cast('[1,2]' as vecf64(2)), cast('[1,2]' as vecf64(2))),
       nullif(cast('[1,2]' as vecf64(2)), cast('[3,4]' as vecf64(2)));
select ifnull(cast(null as vecf16(2)), cast('[1,2]' as vecf16(2))),
       nullif(cast('[1,2]' as vecf16(2)), cast('[1,2]' as vecf16(2))),
       nullif(cast('[1,2]' as vecf16(2)), cast('[3,4]' as vecf16(2)));
select ifnull(cast(null as vecbf16(2)), cast('[1,2]' as vecbf16(2))),
       nullif(cast('[1,2]' as vecbf16(2)), cast('[1,2]' as vecbf16(2))),
       nullif(cast('[1,2]' as vecbf16(2)), cast('[3,4]' as vecbf16(2)));
select ifnull(cast(null as vecint8(2)), cast('[1,2]' as vecint8(2))),
       nullif(cast('[1,2]' as vecint8(2)), cast('[1,2]' as vecint8(2))),
       nullif(cast('[1,2]' as vecint8(2)), cast('[3,4]' as vecint8(2)));
select ifnull(cast(null as vecuint8(2)), cast('[1,2]' as vecuint8(2))),
       nullif(cast('[1,2]' as vecuint8(2)), cast('[1,2]' as vecuint8(2))),
       nullif(cast('[1,2]' as vecuint8(2)), cast('[3,4]' as vecuint8(2)));
select case
       when true then cast('[1,2]' as vecf32(2))
       when true then cast('[3,4]' as vecf32(2))
       else cast('[5,6]' as vecf32(2))
       end;

drop database if exists case_vector_test;
create database case_vector_test;
use case_vector_test;
create table vector_case_ctas as
select case when true
            then cast('[1,2]' as vecf32(2))
            else cast('[3,4]' as vecf32(2))
       end as embedding;
show create table vector_case_ctas;
select embedding from vector_case_ctas;

create table vector_case_rows(
    id int primary key,
    choose_first bool,
    a vecf32(2),
    b vecf32(2)
);
insert into vector_case_rows values
    (1, true,  '[1,2]', '[11,12]'),
    (2, false, '[3,4]', '[13,14]'),
    (3, null,  '[5,6]', '[15,16]'),
    (4, true,  null,    '[17,18]');

select id, case when choose_first then a else b end as selected
from vector_case_rows
order by id;

-- A NULL from strict arithmetic is type-neutral inside common-type consumers,
-- while its standalone projection keeps the resolved numeric type.
select id, null + 1 as numeric_null, coalesce(null + 1, a) as selected
from vector_case_rows order by id;
select id, ifnull(null + 1, a) as selected,
       case when choose_first then a else null + 1 end as conditional
from vector_case_rows order by id;
select coalesce(null + 1, 'x') as text_value,
       if(false, null + 1, cast('2024-01-01' as date)) as date_value;
select n, x from (
    select 1 as n, null + 1 as x
    union all select 2 as n, cast('[1,2]' as vecf32(2)) as x
) u order by n;
select * from (values row(1, null + 1), row(2, cast('[1,2]' as vecf32(2)))) v order by column_0;
create view vector_null_view as select id, null + 1 as x, a from vector_case_rows;
select column_name, column_type from information_schema.columns
where table_schema = 'case_vector_test' and table_name = 'vector_null_view' order by ordinal_position;
select id, x, coalesce(x, a) as selected from vector_null_view order by id;
-- Named-window validation uses a separate binder over the same bound columns.
select id, a from (select id, null + 1 as x, a from vector_case_rows) d
window weak_null_window as (order by coalesce(d.x, d.a)) order by id;
select id, a from (select id, null + 1 as x, a from vector_case_rows) d
window scalar_null_window as (order by coalesce((select d.x), d.a)) order by id;
create function vector_null_identity(x int) returns int language sql as '$1';
select id, coalesce(vector_null_identity(null + 1), a) as selected from vector_case_rows order by id;
create function vector_null_twice(x bigint) returns bigint language sql as
    'coalesce($1, $1, cast(''[1,2]'' as vecf32(2)))';
select vector_null_twice((select null + 1)) as repeated_scalar;
-- Scalar consumers retain their own empty/multiple-row behavior and sources.
select id, coalesce((select null + 1), a) as scalar_value,
       coalesce((select null + 1 where false), a) as empty_scalar
from vector_case_rows order by id;
select id, coalesce((select d.x), d.a) as correlated_scalar
from (select id, null + 1 as x, a from vector_case_rows) d order by id;
select id, coalesce((select (select d.x)), d.a) as nested_correlated_scalar
from (select id, null + 1 as x, a from vector_case_rows) d order by id;
select id, coalesce((select d.x from vector_case_rows q where q.id > d.id limit 0), d.a) as zero_scalar
from (select id, null + 1 as x, a from vector_case_rows) d order by id;
select coalesce((select null + 1 from (select 1 union all select 2) q), a) from vector_case_rows;
select if(false, (select null + 1 from (select 1 union all select 2) q), a) from vector_case_rows;
select coalesce(a, (select null + 1 from (select 1 union all select 2) q)) from vector_case_rows;
select coalesce(x, cast('[1,2]' as vecf32(2)))
from (select (select null + 1 from (select 1 union all select 2) q) as x) d;
select (select null + 1 from (select 1 union all select 2) q)
union all select a from vector_case_rows;
select (select null + 1 from (select 1 union all select 2) q)
union all select null;
select id, x, (select coalesce(d.x, d.x, d.a) from vector_case_rows q limit 1) as correlated
from (select id, null + 1 as x, a from vector_case_rows) d order by id;
select id, (select (select if(false, d.x, d.a))) as deep_correlated
from (select id, null + 1 as x, a from vector_case_rows) d order by id;
select id, (select field(d.x, d.x) from vector_case_rows q limit 1) as null_field,
       (select field(1, d.x) from vector_case_rows q limit 1) as numeric_field
from (select id, null + 1 as x from vector_case_rows) d order by id;
-- A correlated use must retain its capture even when its value is always NULL.
create sequence vector_null_seq start with 1;
select count(distinct r) as values_seen from (
    select (select coalesce(d.x, nextval('vector_null_seq'))) as r
    from (select id, null + 1 as x from vector_case_rows) d
) q;
select currval('vector_null_seq');
drop sequence vector_null_seq;
-- A mixed NULL-only set keeps the old concrete TEXT placeholder metadata.
-- @metacmp(true)
select coalesce(d.x, 'x') as mixed_text from (select null as x union all select null + 1) d;
-- @metacmp(true)
select coalesce(d.x, cast('abcd' as binary(4))) as mixed_binary
from (select null + 1 as x union all select null) d;
-- An explicit numeric NULL, a different NULL producer, and mismatched vector
-- dimensions keep their existing admission errors.
select coalesce(cast(null as signed), a) from vector_case_rows;
select coalesce((select cast(null + 1 as signed)), a) from vector_case_rows;
select coalesce(1 / 0, a) from vector_case_rows;
select coalesce(null + 1, a, cast('[1,2,3]' as vecf32(3))) from vector_case_rows;
create table vector_null_twin (x bigint, v vecf32(2));
insert into vector_null_twin values (null, '[1,2]');
select coalesce(x, v) from vector_null_twin;
prepare vector_null_stmt from 'select coalesce(null + 1, a) from vector_case_rows where id = ?';
set @vector_id = 1;
execute vector_null_stmt using @vector_id;
set @vector_id = 4;
execute vector_null_stmt using @vector_id;
deallocate prepare vector_null_stmt;
prepare vector_null_union from 'select null + 1 as x union all select ?';
set @vector_null_param = 7;
execute vector_null_union using @vector_null_param;
set @vector_null_param = 'x';
execute vector_null_union using @vector_null_param;
set @vector_null_param = null;
execute vector_null_union using @vector_null_param;
deallocate prepare vector_null_union;

select id,
       case when id = 1
            then case when choose_first then a else b end
            else b
       end as nested_selected
from vector_case_rows
order by id;

prepare vector_case_stmt from
    'select case when ? then cast(''[1,2]'' as vecf32(2)) else cast(''[3,4]'' as vecf32(2)) end';
set @condition = true;
execute vector_case_stmt using @condition;
set @condition = false;
execute vector_case_stmt using @condition;
set @condition = null;
execute vector_case_stmt using @condition;
set @condition = true;
execute vector_case_stmt using @condition;
deallocate prepare vector_case_stmt;

-- Mixed vector branches take the common type of docs/sql/vector-common-type.md;
-- branches of different dimensions fail even when a constant condition makes one
-- branch unreachable.
select case when true then cast('[1,2]' as vecf32(2)) else cast('[3,4]' as vecf64(2)) end;
select case when true then cast('[1,2]' as vecf32(2)) else cast('[3,4,5]' as vecf32(3)) end;
select case when true then cast('[1,2]' as vecf32(2)) else '[3,4]' end;

drop database case_vector_test;
