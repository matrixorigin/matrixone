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

-- Incompatible vector result branches must fail even when a constant condition
-- would make one branch unreachable.
select case when true then cast('[1,2]' as vecf32(2)) else cast('[3,4]' as vecf64(2)) end;
select case when true then cast('[1,2]' as vecf32(2)) else cast('[3,4,5]' as vecf32(3)) end;
select case when true then cast('[1,2]' as vecf32(2)) else '[3,4]' end;

drop database case_vector_test;
