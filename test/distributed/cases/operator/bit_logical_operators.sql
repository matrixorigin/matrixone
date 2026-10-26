-- @suite

-- @case
-- @desc: BIT columns in AND/OR/XOR/NOT: non-zero is TRUE, zero is FALSE, NULL stays NULL (MySQL semantics)
-- @label:bvt

drop database if exists bit_logical_operators;
create database bit_logical_operators;
use bit_logical_operators;

create table tb(id int primary key, b bit(1), w bit(8));
insert into tb values (1, 0, 0), (2, 1, 1), (3, 1, 128), (4, null, null);

select id, b and 1 as b_and, b or 0 as b_or, not b as not_b, b xor 1 as b_xor from tb order by id;
select id, w and 1 as w_and, w or 0 as w_or, not w as not_w, w xor 0 as w_xor from tb order by id;
select id, b && w as b_and_w, b || w as b_or_w, !w as bang_w from tb order by id;
select id from tb where not w order by id;
select id from tb where b and w order by id;
select id from tb where b or w is null order by id;
select id, case when w and true then 'y' else 'n' end as case_w from tb order by id;
select id, if(not b, 'f', 't') as if_not_b, b + 0 as b_num, w + 0 as w_num from tb order by id;

drop database if exists bit_logical_operators;
