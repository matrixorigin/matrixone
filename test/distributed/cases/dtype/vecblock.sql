-- #20567: block-scaled vector columns vecf8 (MXFP8) and vecf4 (NVFP4).
drop database if exists vecblock_db;
create database vecblock_db;
use vecblock_db;

create table t (id int primary key, a vecf8(4), b vecf4(4), c vecf32(4));
show create table t;
desc t;

insert into t values (1, '[1,-3,0,6]', '[1,-3,0,6]', '[1,-3,0,6]');
insert into t values (2, '[0.5,0.25,-0.75,1]', '[0.5,0.25,-0.75,1]', '[2,1,-1,0.5]');
insert into t values (3, null, null, null);
insert into t (id, a, b) values (4, '[3000,-12,0.001,1000000]', '[3000,-12,0.001,1000000]');
-- flush writes the block (zonemap bounds for every column)
-- @ignore:0
select mo_ctl('dn', 'flush', 'vecblock_db.t');
select * from t order by id;

-- casts
select cast('[1,2,3]' as vecf8(3)), cast('[1,2,3]' as vecf4(3));
select id, cast(a as vecf32(4)), cast(b as vecf32(4)) from t order by id;
select id, cast(c as vecf8(4)), cast(c as vecf4(4)) from t order by id;
select cast(a as vecf4(4)), cast(b as vecf8(4)) from t where id = 1;

-- rejected values
insert into t values (5, '[1,2,3]', '[1,2,3,4]', '[1,2,3,4]');
insert into t values (5, '[1,2,3,4]', '[1,2,3]', '[1,2,3,4]');
select cast('[1,2,3]' as vecf8(4));
insert into t (id, a) values (5, '[1,2,3,nan]');
insert into t (id, b) values (5, '[1,2,3,inf]');
-- a finite value whose quantized vecf8 value decodes to Inf is rejected; vecf4 keeps it finite
select cast('[3.4028235e38]' as vecf8(1));
select cast('[-3.4028235e38, 1]' as vecf8(2));
select cast('[3.4028235e38]' as vecf4(1));

-- arithmetic promotes to vecf32
select a + a, a * 2, b - c, a / 2, a + b from t where id = 1;

-- inner_product (MO returns the negated dot product; c is the vecf32 control)
select id, inner_product(c, c), inner_product(a, c), inner_product(b, c), inner_product(a, b), inner_product(a, '[1,1,1,1]') from t order by id;

-- distances: each block-scaled column against itself, the other format, vecf32 and a literal
select id, l2_distance(c, c), l2_distance(a, c), l2_distance(b, c), l2_distance(a, b), l2_distance(a, '[1,1,1,1]') from t order by id;
select id, l2_distance_sq(a, c), l2_distance_sq(b, '[1,1,1,1]'), l2_distance_sq(c, b) from t order by id;
select id, l1_distance(a, c), l1_distance(b, c), l1_distance(a, b), l1_distance('[1,1,1,1]', b) from t order by id;
select id, cosine_distance(a, c), cosine_distance(b, c), cosine_distance(a, b), cosine_distance(a, a) from t order by id;
select id, cosine_similarity(a, c), cosine_similarity(c, b), cosine_similarity(a, '[1,1,1,1]') from t order by id;
select id, vector_dims(a), vector_dims(b), normalize_l2(a), normalize_l2(b) from t order by id;
select id from t where l2_distance(a, '[1,-3,0,6]') < 1 order by id;
select id from t order by l2_distance(b, '[1,1,1,1]'), id limit 2;
select l2_distance(a, '[1,2,3]') from t where id = 1;
select cosine_similarity(a, '[0,0,0,0]') from t where id = 1;
select cosine_distance(a, '[0,0,0,0]') from t where id = 1;

-- float32 lane overflow: +Inf and -Inf lanes would sum to NaN; the distance is +Inf, an overflow error
select inner_product(cast('[3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38]' as vecf8(32)), cast('[3e38,-3e38,3e38,-3e38,3e38,-3e38,3e38,-3e38,3e38,-3e38,3e38,-3e38,3e38,-3e38,3e38,-3e38,3e38,-3e38,3e38,-3e38,3e38,-3e38,3e38,-3e38,3e38,-3e38,3e38,-3e38,3e38,-3e38,3e38,-3e38]' as vecf8(32)));
select cosine_distance(cast('[3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38]' as vecf4(32)), cast('[3e38,-3e38,3e38,-3e38,3e38,-3e38,3e38,-3e38,3e38,-3e38,3e38,-3e38,3e38,-3e38,3e38,-3e38,3e38,-3e38,3e38,-3e38,3e38,-3e38,3e38,-3e38,3e38,-3e38,3e38,-3e38,3e38,-3e38,3e38,-3e38]' as vecf4(32)));
select l2_distance(cast('[3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38,3e38]' as vecf8(32)), cast('[3e38,-3e38,3e38,-3e38,3e38,-3e38,3e38,-3e38,3e38,-3e38,3e38,-3e38,3e38,-3e38,3e38,-3e38,3e38,-3e38,3e38,-3e38,3e38,-3e38,3e38,-3e38,3e38,-3e38,3e38,-3e38,3e38,-3e38,3e38,-3e38]' as vecf8(32)));

-- ordering by value and grouping, as for vecf32
select id from t order by a, id;
select id from t order by b desc, id;
select id from t order by c, id;
select a, count(*) from t group by a order by a;
select count(distinct b) from t;
select id, rank() over (partition by a order by id) from t order by id;

-- aggregates
select count(a), count(b) from t;
select group_concat(a order by id separator ';') from t;
select any_value(b) from t where id = 1;

-- updates and schema change
update t set a = '[4,3,2,1]', b = '[4,3,2,1]' where id = 2;
select a, b from t where id = 2;
alter table t add column d vecf4(4) not null;
select id, d from t order by id;

-- prepared statement
prepare s from 'select id from t where inner_product(a, ?) < 0 order by id';
set @q = '[1,1,1,1]';
execute s using @q;
deallocate prepare s;
prepare s2 from 'select id, cosine_distance(b, ?) from t where l2_distance(b, ?) < 5 order by id';
execute s2 using @q, @q;
deallocate prepare s2;

-- NULL handling, conditionals, element math and JSON dequantize to vecf32
select id, summation(a), l1_norm(b), l2_norm(a) from t where id in (1, 2) order by id;
select id, abs(a), sqrt(abs(b)) from t where id = 1;
select id, coalesce(a, '[0,0,0,0]'), greatest(a, b), case when id = 1 then a else b end from t where id in (1, 3) order by id;
select json_object('a', a), json_array(b) from t where id = 1;

-- comparisons as for the other narrow vector types: a literal is quantized to the column's
-- type, as the stored value was, and cells compare by their dequantized values element-wise
select id from t where a = '[3000,-12,0.001,1000000]';
select id from t where b = '[3000,-12,0.001,1000000]';
select id from t where a = '[3072,-16,0,983040]';
select id from t where a < '[1,1,1,1]' order by id;
select id from t where b >= '[1,-3,0,6]' order by id;
select id from t where a in ('[1,-3,0,6]', '[4,3,2,1]') order by id;
select id from t where a = a order by id;
select id from t where b between '[0,0,0,0]' and '[2,2,2,2]' order by id;

-- not supported (vecf32-only, as for the other narrow vector types)
select hex(a) from t;
select sum(a) from t;
select avg(b) from t;
create index idx on t(a);
create unique index uidx on t(b);
create table t2 (a vecf8(4) primary key);
create table t3 (id int primary key, v vecf4(65536));

drop database vecblock_db;
