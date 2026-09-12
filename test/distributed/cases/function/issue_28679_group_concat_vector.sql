-- issue #28679: GROUP_CONCAT must use the public text representation for
-- vector values instead of exposing their internal numeric bytes.
drop table if exists issue_28679_group_concat_vector;
create table issue_28679_group_concat_vector (
    id int primary key,
    v32 vecf32(3),
    v64 vecf64(3),
    vbf vecbf16(3),
    vf16 vecf16(3),
    vi8 vecint8(3),
    vu8 vecuint8(3)
);
insert into issue_28679_group_concat_vector values
    (1, '[1, 2, 3]', '[1, 2, 3]', '[1, 2, 3]', '[1, 2, 3]', '[-1, 0, 4]', '[1, 2, 3]'),
    (2, '[1, 2, 3]', '[1, 2, 3]', '[1, 2, 3]', '[1, 2, 3]', '[-1, 0, 4]', '[1, 2, 3]'),
    (3, '[-1, 0, 4]', '[-1, 0, 4]', '[-1, 0, 4]', '[-1, 0, 4]', '[-1, 0, 4]', '[4, 5, 6]'),
    (4, null, null, null, null, null, null);

-- Direct SELECT is the public formatting control for every vector family.
select v32, v64, vbf, vf16, vi8, vu8
from issue_28679_group_concat_vector
where id = 1;

-- Ordinary GROUP_CONCAT must format each vector before joining it.
select group_concat(v32 separator '|'),
       group_concat(v64 separator '|'),
       group_concat(vbf separator '|'),
       group_concat(vf16 separator '|'),
       group_concat(vi8 separator '|'),
       group_concat(vu8 separator '|')
from issue_28679_group_concat_vector
where id = 1;

-- ORDER BY must retain the raw payload for ordering and format at emission.
select group_concat(v32 order by id separator '|'),
       group_concat(v64 order by id separator '|'),
       group_concat(vbf order by id separator '|'),
       group_concat(vf16 order by id separator '|'),
       group_concat(vi8 order by id separator '|'),
       group_concat(vu8 order by id separator '|')
from issue_28679_group_concat_vector
where id <= 3;

-- DISTINCT must deduplicate raw vector values, not their byte-buffer display.
select group_concat(distinct v32 order by id separator '|'),
       group_concat(distinct v64 order by id separator '|'),
       group_concat(distinct vbf order by id separator '|'),
       group_concat(distinct vf16 order by id separator '|'),
       group_concat(distinct vi8 order by id separator '|'),
       group_concat(distinct vu8 order by id separator '|')
from issue_28679_group_concat_vector
where id <= 3;

-- Multiple arguments must format each vector independently.
select group_concat(v32, ':', vu8 order by id separator '|')
from issue_28679_group_concat_vector
where id <= 3;

-- NULL vector rows remain skipped, and an all-NULL group remains SQL NULL.
select group_concat(v32 order by id separator '|')
from issue_28679_group_concat_vector
where id in (1, 4);
select group_concat(v32)
from issue_28679_group_concat_vector
where id = 4;

-- group_concat_max_len applies to the rendered text, not the raw bytes.
set session group_concat_max_len = 12;
select group_concat(v32 order by id separator '|')
from issue_28679_group_concat_vector
where id in (1, 3);
show warnings;
set session group_concat_max_len = 1024;

drop table issue_28679_group_concat_vector;
