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

-- float32 lane overflow: +Inf and -Inf lanes would sum to NaN; inner product and L2 are +Inf, an
-- overflow error; cosine recomputes in float64
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

-- a string compared with vecf8/vecf4 is cast to the column's dimension
create table dimchk (x vecf8(4), y vecf4(4));
insert into dimchk values ('[1,2,3,4]', '[1,2,3,4]');
select count(*) from dimchk where x = '[1,2,3]';
select count(*) from dimchk where y in ('[1,2,3,4,5]');
select count(*) from dimchk where x = '[1,2,3,4]';
-- IF/CASE/COALESCE keep the type; a multi-table UPDATE stores the cells
create table mu1 (id int primary key, e vecf8(4), f vecf4(4));
insert into mu1 values (1, '[1,2,3,4]', '[1,2,3,4]');
create table mu2 (k int, e vecf8(4), f vecf4(4));
insert into mu2 values (1, '[4,3,2,1]', '[6,4,2,1]');
update mu1 join mu2 on mu1.id = mu2.k set mu1.e = mu2.e, mu1.f = mu2.f;
select * from mu1;
select if(id > 0, e, null), coalesce(null, f), case when id > 0 then e end from mu1;
-- data branch merge of an updated vecf8/vecf4 row
create table va (id int primary key, v vecf8(4), u vecf4(3));
insert into va values (1, '[1,2,3,4]', '[1,2,3]');
data branch create table vb from va;
data branch create table vc from va;
update vb set v = '[4,3,2,1]', u = '[3,2,1]' where id = 1;
data branch merge vb into vc;
select * from vc;

-- a case branch casts only the rows it selects
create table sel (a int, s varchar(20));
insert into sel values (1, '[1,2]'), (0, '[1,2,3]'), (2, 'invalid');
select a, case when a = 1 then cast(s as vecf4(2)) end, case when a = 1 then cast(s as vecf8(2)) end from sel order by a;

-- binary vector input: a BLOB of little-endian float32 elements, as for vecf32
create table bin (id int, b vecf4(2), e vecf8(2), a vecf32(2));
insert into bin values (1, cast(unhex('0000803F00000040') as blob), cast(unhex('0000803F00000040') as blob), cast(unhex('0000803F00000040') as blob));
insert into bin (id, b) values (2, cast(unhex('0000803F000000') as blob));
insert into bin (id, e) values (3, cast(unhex('0000803F') as blob));
select * from bin order by id;

-- CAST(X'...' AS BLOB) in VALUES is the binary vector input for every vector type
create table hexin (id int primary key, a vecf32(2), h vecbf16(2), e vecf8(2), b vecf4(2));
insert into hexin values (1, cast(X'0000803F00000040' as blob), cast(X'803F0040' as blob), cast(X'0000803F00000040' as blob), cast(X'0000803F00000040' as blob));
insert into hexin (id, a, b) values (2, cast(X'00004040000080C0' as blob), cast(X'00004040000080C0' as blob)), (3, cast(X'0000003F0000A040' as blob), cast(X'0000003F0000A040' as blob));
replace into hexin (id, e) values (3, cast(X'0000803F00000040' as blob));
select * from hexin order by id;
-- a literal cast in VALUES for a non-vector column keeps the column as its binding type
create table hexkeep (id int, c varchar(10), d decimal(5,2));
insert into hexkeep values (1, cast(X'3132' as signed), cast(1.005 as double));
select * from hexkeep;

-- cells that encode equal values with other scales are one key: GROUP BY, DISTINCT, joins
-- and set operations agree with = (447 and 449 both store 448 in vecf8)
create table eqk (id int, v4 vecf4(1), v8 vecf8(1));
insert into eqk values (1, '[1.2031566]', '[447]'), (2, '[1.2031565]', '[449]');
select v8 = (select v8 from eqk where id = 1), v4 = (select v4 from eqk where id = 1) from eqk where id = 2;
select v8, count(*) from eqk group by v8;
select count(*) from (select v4 from eqk group by v4) x;
select count(distinct v4), count(distinct v8), approx_count_distinct(v8) from eqk;
select count(*) from eqk a join eqk b on a.v8 = b.v8;
select count(*) from eqk a join eqk b on a.v4 = b.v4;
select count(*) from (select v8 from eqk where id = 1 union select v8 from eqk where id = 2) u;
select count(*) from eqk where v8 in (select v8 from eqk where id = 2);
select count(*) from (select sample(v8, 1 rows) from eqk) s;

-- the exact text: vecblock_json returns the cell as stored and casts back to the same cell;
-- the decoded text is quantized again (a vecf4 global scale follows the decoded maximum)
create table exact (id int primary key, v vecf4(17), e vecf8(3));
insert into exact values (1, '[8.7649145,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,5.7432985]', '[447,1,-2]');
select vecblock_json(v), vecblock_json(e) from exact;
insert into exact select 2, vecblock_json(v), vecblock_json(e) from exact where id = 1;
insert into exact values (3, '[8.764914, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6.2606535]', '[448, 1, -2]');
select id, v, vecblock_json(v) = (select vecblock_json(v) from exact where id = 1) same_cell from exact order by id;
select vecblock_json(cast(null as vecf8(2)));
select cast('{"g":1,"s":[1],"v":[1,2]}' as vecf4(3));
select cast('{"g":1,"s":[3],"v":[1]}' as vecf8(1));
select cast('{"g":1,"s":[1],"v":[2.5]}' as vecf4(1));
select cast('{"s":[1],"v":[1]}' as vecf8(1));
select cast('{"g":1,"s":[1],"v":[1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]}' as vecf4(17));

-- cells with equal values and different bytes are peers in ORDER BY and windows
create table peer (id int, v vecf8(1));
insert into peer values (2, '{"g":1,"s":[1],"v":[1]}'), (1, '{"g":1,"s":[2],"v":[0.5]}'), (0, '{"g":1,"s":[1],"v":[1]}'), (3, '[2]');
select id from peer order by v, id;
select id, rank() over (order by v) r, count(*) over (partition by v) c from peer order by id;

-- a user variable keeps the cell
create table uv (id int, v vecf4(17), e vecf8(2));
insert into uv values (1, '[8.7649145,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,5.7432985]', '[447, -1.5]');
set @v = (select v from uv where id = 1);
set @e = (select e from uv where id = 1);
select @v, @e;
insert into uv values (2, @v, @e);
select a.id, b.id, vecblock_json(a.v) = vecblock_json(b.v), vecblock_json(a.e) = vecblock_json(b.e) from uv a, uv b where a.id = 1 and b.id = 2;
select count(*) from uv where v = @v and e = @e;

-- vecblock_binary returns the stored cell; a BLOB of a cell casts back to the same bytes
create table vbin (id int, v vecf4(17), e vecf8(2));
insert into vbin values (1, '[8.7649145,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,5.7432985]', '[447, -1.5]');
select hex(vecblock_binary(v)), hex(vecblock_binary(e)), length(vecblock_binary(v)), length(vecblock_binary(e)) from vbin;
insert into vbin select 2, cast(vecblock_binary(v) as vecf4(17)), cast(vecblock_binary(e) as vecf8(2)) from vbin where id = 1;
insert into vbin values (3, cast(unhex('01020000110000006CB2553B7E7A070000000000000007') as blob), cast(unhex('01010000020000000000803F7F7EBC') as blob));
select id, vecblock_json(v), vecblock_json(e) from vbin order by id;
select count(distinct vecblock_binary(v)), count(distinct vecblock_binary(e)) from vbin;
select vecblock_binary(cast(vecblock_binary(e) as vecf8)) = vecblock_binary(e) from vbin where id = 1;
-- a BLOB of cell length that is not a valid cell is an error
-- an unsized cast of a cell of the other format is an error
select vecblock_json(cast(vecblock_binary(cast('[1,2,3,4,5]' as vecf4(5))) as vecf8));
select cast(cast(unhex('02010000020000000000803F7F7EBC') as blob) as vecf8(2));
select cast(cast(unhex('01020000020000000000803F7F7EBC') as blob) as vecf8(2));
select cast(cast(unhex('01010000020000000000803F7F7EBC') as blob) as vecf8(3));
select vecblock_binary(cast('[1,2]' as vecf32(2)));

-- Parquet: binary columns (no logical type) hold the stored cell or float32 elements,
-- STRING columns the vecblock JSON or '[...]' text, LIST columns float arrays
create table pq (id int, v4_bin vecf4(17), v8_bin vecf8(33), v4_f32 vecf4(17), v4_json vecf4(17), v8_text vecf8(33), v4_list vecf4(17), v8_list vecf8(33));
load data infile {'filepath'='$resources/parquet/vecblock.parquet', 'format'='parquet'} into table pq;
select id, v4_bin, v8_bin from pq order by id;
select id, vecblock_binary(v4_bin) = vecblock_binary(v4_f32), vecblock_binary(v4_bin) = vecblock_binary(v4_json), vecblock_binary(v8_bin) = vecblock_binary(v8_text) from pq order by id;
select id, vecblock_binary(v4_list) = vecblock_binary(v4_bin), vecblock_binary(v8_list) = vecblock_binary(v8_bin), v4_list is null, v8_list is null from pq order by id;
select count(*) from pq where v4_bin = cast('[8.7649145,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,5.7432985]' as vecf4(17));
create external table pqx (id int, v4_bin vecf4(17), v8_bin vecf8(33), v4_f32 vecf4(17), v4_json vecf4(17), v8_text vecf8(33), v4_list vecf4(17), v8_list vecf8(33)) infile{'filepath'='$resources/parquet/vecblock.parquet', 'format'='parquet'};
select count(*) from pqx x join pq p on x.id = p.id where vecblock_binary(x.v4_bin) = vecblock_binary(p.v4_bin) and vecblock_binary(x.v8_bin) = vecblock_binary(p.v8_bin);
-- a binary column is not read as text, and a cell must match the declared dimension
create table pqbad (id int, v4_bin vecf4(16), v8_bin vecf8(33), v4_f32 vecf4(17), v4_json vecf4(17), v8_text vecf8(33), v4_list vecf4(17), v8_list vecf8(33));
load data infile {'filepath'='$resources/parquet/vecblock.parquet', 'format'='parquet'} into table pqbad;

-- export writes the exact text; CSV and JSONL reload to the same cells
create table expt (id int, v vecf4(17), e vecf8(2));
insert into expt values (1, '[8.7649145,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,5.7432985]', '[447, -1.5]'), (2, null, '[0.001, 3]');
select * from expt order by id into outfile '$resources/into_outfile/vecblock.csv';
select load_file(cast('file://$resources/into_outfile/vecblock.csv' as datalink)) as csv_content;
select * from expt order by id into outfile '$resources/into_outfile/vecblock.jsonl';
select load_file(cast('file://$resources/into_outfile/vecblock.jsonl' as datalink)) as jsonl_content;
create table expt_csv like expt;
load data infile '$resources/into_outfile/vecblock.csv' into table expt_csv fields terminated by ',' enclosed by '"' ignore 1 lines;
create table expt_jsonl like expt;
load data infile {'filepath'='$resources/into_outfile/vecblock.jsonl', 'format'='jsonline', 'jsondata'='object'} into table expt_jsonl;
select x.id, vecblock_json(x.v) <=> vecblock_json(c.v), vecblock_json(x.e) = vecblock_json(c.e), vecblock_json(x.v) <=> vecblock_json(j.v), vecblock_json(x.e) = vecblock_json(j.e) from expt x join expt_csv c on x.id = c.id join expt_jsonl j on x.id = j.id order by x.id;
select id, v, e from expt_jsonl order by id;

-- cells equal by decoded value can differ in bytes: vecf8/vecf4 are no cluster by or KEY
-- partition columns
create table cb8 (id int, v vecf8(4)) cluster by (v);
create table cb4 (id int, v vecf4(4)) cluster by (id, v);
create table pk8 (id int, v vecf8(4)) partition by key(v) partitions 2;
create table pk4 (id int, v vecf4(4)) partition by key(id, v) partitions 2;

-- json_row writes the decoded values; a vecf8 equality key partitions a correlated LIMIT
select json_row(cast('[1,-2,0.5]' as vecf8(3)), cast('[1,-2,0.5]' as vecf4(3)), cast(null as vecf8(3)));
create table cl (id int, v vecf8(2));
insert into cl values (1, '[1,2]'), (2, '[1,2]'), (3, '[3,4]');
select a.id, (select b.id from cl b where b.v = a.v order by b.id desc limit 1) m from cl a order by a.id;
drop table cl;

-- a distance to a vecf8 query vector is not the vecf32 index score of the same text
set experimental_ivf_index = 1;
create table iv (id int primary key, v vecf32(3));
insert into iv values (1, '[1,2,3]'), (2, '[1.3,2.7,3.1]'), (3, '[4,5,6]'), (4, '[0,0,1]');
create index ix using ivfflat on iv(v) lists = 1 op_type 'vector_l2_ops';
select id, l2_distance(v, cast('[1.3,2.7,3.1]' as vecf8(3))) dq8 from iv order by l2_distance(v, '[1.3,2.7,3.1]') limit 3;
drop table iv;
set experimental_ivf_index = 0;

-- percentile_disc sorts with unaccounted scratch: no vector type, as for vecf32
select percentile_disc(0.5) within group (order by a) from t;
select percentile_disc(0.5) within group (order by c) from t;
drop database vecblock_db;
