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
    v varchar(8)
);
insert into pad_char_to_full_length_t values ('MO', '你好', 'MO');

select char_length(c), length(c), hex(c), concat('>', c, '<'),
       char_length(unicode_c), length(unicode_c), hex(unicode_c),
       char_length(v), length(v), hex(v)
from pad_char_to_full_length_t;
select count(*) from pad_char_to_full_length_t where length(c) = 2;

prepare pad_char_stmt from
    'select c, char_length(c), length(c), hex(c), concat(''>'', c, ''<'') from pad_char_to_full_length_t';

set sql_mode = 'PAD_CHAR_TO_FULL_LENGTH';
select char_length(c), length(c), hex(c), concat('>', c, '<'),
       char_length(unicode_c), length(unicode_c), hex(unicode_c),
       char_length(v), length(v), hex(v)
from pad_char_to_full_length_t;
select count(*) from pad_char_to_full_length_t where length(c) = 8;
execute pad_char_stmt;

set sql_mode = '';
execute pad_char_stmt;

deallocate prepare pad_char_stmt;
set sql_mode = @saved_sql_mode;
drop table pad_char_to_full_length_t;
