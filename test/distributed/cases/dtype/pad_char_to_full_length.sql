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
create table pad_char_set8 (c char(8));
create table pad_char_set4 (c char(4));
insert into pad_char_set8 values ('MO');
insert into pad_char_set4 values ('MO');

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
select length(c), hex(c) from (select c from pad_char_set8 union select c from pad_char_set4) u;
execute pad_char_stmt;

set sql_mode = '';
execute pad_char_stmt;

deallocate prepare pad_char_stmt;
set sql_mode = @saved_sql_mode;
drop table pad_char_to_full_length_t;
drop table pad_char_set8;
drop table pad_char_set4;
