-- docs/sql/vector-distance-functions.md: every table row of that page is checked here.
drop database if exists vec_distance_semantics;
create database vec_distance_semantics;
use vec_distance_semantics;

-- distances: smaller is nearer; inner_product is the negated dot product
select inner_product('[1,2,3]', '[4,5,6]') ip, cosine_similarity('[1,2,3]', '[4,5,6]') cs, cosine_distance('[1,2,3]', '[4,5,6]') cd, l2_distance('[1,2,3]', '[4,5,6]') l2, l2_distance_sq('[1,2,3]', '[4,5,6]') l2sq, l1_distance('[1,2,3]', '[4,5,6]') l1;
select inner_product('[1,2]', '[-3,-4]') far_apart;

-- zero vectors
select inner_product('[0,0]', '[1,2]') ip, cosine_distance('[0,0]', '[1,2]') cd, cosine_distance('[0,0]', '[0,0]') cd_both;
select cosine_similarity('[0,0]', '[1,2]');

-- NULL, dimension mismatch, overflow, empty vector
select inner_product(null, '[1,2]') ip, l2_distance('[1,2]', null) l2;
select l2_distance('[1,2]', '[1,2,3]');
select cosine_distance('[1,2]', '[1,2,3]');
select inner_product('[1,2]', '[1,2,3]');
select inner_product(cast('[1,2]' as vecbf16(2)), cast('[1,2,3]' as vecbf16(3)));
select l2_distance(cast('[1,2]' as vecint8(2)), cast('[1,2,3]' as vecint8(3)));
select l1_distance(cast('[1,2]' as vecf16(2)), cast('[1,2,3]' as vecf16(3)));
select cosine_distance(cast('[1,2]' as vecuint8(2)), cast('[1,2,3]' as vecuint8(3)));
select l2_distance_sq('[1e20,0]', '[0,0]');
select inner_product('[1e30,1e30]', '[1e30,1e30]');
select l2_distance('[]', '[]');
select inner_product(cast('[1,2]' as vecf64(2)), cast('[3,4]' as vecf32(2))) mixed;
-- every distance but l2_distance_sq is rounded into the float32 domain
select l2_distance(cast('[3.1]' as vecf64(1)), cast('[0]' as vecf64(1))) l2, l2_distance_sq(cast('[3.1]' as vecf64(1)), cast('[0]' as vecf64(1))) l2sq;
select normalize_l2('[0,0]') zero;

-- result types of every function over every vector type
create table v (id int, a32 vecf32(3), a64 vecf64(3), ab vecbf16(3), ah vecf16(3), ai vecint8(3), au vecuint8(3));
insert into v values (1, '[3,4,12]', '[3,4,12]', '[3,4,12]', '[3,4,12]', '[3,4,12]', '[3,4,12]');
create table r as select
  inner_product(a32,a32) ip32, inner_product(a64,a64) ip64, inner_product(ab,ab) ipb, inner_product(ah,ah) iph, inner_product(ai,ai) ipi, inner_product(au,au) ipu,
  cosine_distance(ai,ai) cdi, l2_distance(au,au) l2u, l2_distance_sq(ab,ab) l2sqb, l1_distance(ah,ah) l1h, cosine_similarity(ai,ai) csi,
  summation(a32) s32, summation(ai) si, l1_norm(au) l1nu, l2_norm(ab) l2nb, vector_dims(ah) dh,
  normalize_l2(a32) n32, normalize_l2(a64) n64, normalize_l2(ab) nb, normalize_l2(ah) nh, normalize_l2(ai) ni, normalize_l2(au) nu,
  subvector(a32,2) sv32, subvector(a64,2) sv64, subvector(ab,2) svb, subvector(ah,2) svh, subvector(ai,2,1) svi, subvector(au,2,1) svu,
  abs(a32) abs32, abs(a64) abs64, sqrt(a32) sqrt32, sqrt(a64) sqrt64,
  a32 + a32 add32, a64 - a64 sub64, a32 * 2 mul32, a64 / a64 div64,
  coalesce(ai, ai) coi, if(id = 1, ab, ab) ifb, case when id = 1 then au else au end caseu,
  hex(a32) hex32, to_base64(a64) b64
from v;
select column_name, data_type from information_schema.columns where table_schema = 'vec_distance_semantics' and table_name = 'r' order by ordinal_position;
select ip32, ipi, s32, si, l2nb, dh, n32, nb, nh, ni, nu, svi, svu, sqrt32 from r;
select a32 = a32, ab < ab, ai in (ai), au <> au, ah <= ah, a64 > a64, ai >= ai, a32 in (a64) from v;
create table r2 as select coalesce(a32, a32) c32, coalesce(a64, a64) c64, coalesce(ab, ab) cb, coalesce(ah, ah) ch, coalesce(au, au) cu,
  if(id = 1, a32, a32) i32, if(id = 1, a64, a64) i64, if(id = 1, ah, ah) ih, if(id = 1, ai, ai) ii, if(id = 1, au, au) iu,
  case when id = 1 then a32 end k32, case when id = 1 then a64 end k64, case when id = 1 then ab end kb, case when id = 1 then ah end kh, case when id = 1 then ai end ki
from v;
select column_name, data_type from information_schema.columns where table_schema = 'vec_distance_semantics' and table_name = 'r2' order by ordinal_position;
create table r3 as select any_value(a32) y32, any_value(a64) y64, any_value(ab) yb, any_value(ah) yh, any_value(ai) yi, any_value(au) yu from v;
select column_name, data_type from information_schema.columns where table_schema = 'vec_distance_semantics' and table_name = 'r3' order by ordinal_position;

-- narrow types: no abs, sqrt, arithmetic, hex or to_base64
select abs(ab) from v;
select sqrt(ah) from v;
select ai + ai from v;
select au * 2 from v;
select hex(ab) from v;
select to_base64(ai) from v;
select max(a32) from v;
select min(ai) from v;
select sum(a64) from v;
select avg(ab) from v;

-- casts between vector types; an out-of-range element is an error
select cast(a32 as vecint8(3)) i8, cast(ab as vecf64(3)) f64, cast(ai as vecbf16(3)) bf from v;
select cast(cast('[-300,300,1.6,-1.6]' as vecf32(4)) as vecint8(4)) i8, cast(cast('[-1,300,1.6]' as vecf32(3)) as vecuint8(3)) u8;
select cast(cast('[70000]' as vecf32(1)) as vecf16(1));
insert into v values (2, '[3,4,12]', '[3,4,12]', '[3,4,12]', '[3,4,12]', '[3,4,12]', '[3,4,12]');
select count(distinct a32), count(distinct a64), count(distinct ab), count(distinct ah), count(distinct ai), count(distinct au) from v;
select a32, count(*) from v group by a32;
select ah, count(*) from v group by ah;
select au, count(*) from v group by au;

-- index search: each op_type serves its functions, ascending only, with the scalar function's values
set experimental_ivf_index = 1;
set experimental_hnsw_index = 1;
create table t (id bigint primary key, v vecf32(3));
insert into t values (1,'[0.5,0.5,-3]'),(2,'[-0.5,-1,2]'),(3,'[-2,-3,-1]'),(4,'[-0.5,3,0.5]'),(5,'[-3,1,0.5]'),(6,'[2,-2,-2]'),(7,'[-2,-0.5,-0.5]'),(8,'[-3,1,1]');
select id, round(inner_product(v, '[1,2,3]'), 5) ip, round(l2_distance(v, '[1,2,3]'), 5) l2, round(l2_distance_sq(v, '[1,2,3]'), 5) l2sq, round(cosine_distance(v, '[1,2,3]'), 5) cd, round(l1_distance(v, '[1,2,3]'), 5) l1 from t order by id;

create index ix using ivfflat on t(v) lists = 2 op_type 'vector_l2_ops';
-- @separator:table
-- @regex("Vector Index Scan on ix \[l2_distance\]", true)
explain select id from t order by l2_distance(v, '[1,2,3]') limit 3;
select id, round(l2_distance(v, '[1,2,3]'), 5) d from t order by l2_distance(v, '[1,2,3]') limit 3;
-- @separator:table
-- @regex("Vector Index Scan on ix \[l2_distance_sq\]", true)
explain select id from t order by l2_distance_sq(v, '[1,2,3]') limit 3;
select id, round(l2_distance_sq(v, '[1,2,3]'), 5) d from t order by l2_distance_sq(v, '[1,2,3]') limit 3;
-- @separator:table
-- @regex("Vector Index Scan", false)
explain select id from t order by l2_distance(v, '[1,2,3]') desc limit 3;
drop index ix on t;
create index ix using ivfflat on t(v) lists = 2 op_type 'vector_l2sq_ops';
-- @separator:table
-- @regex("Vector Index Scan on ix \[l2_distance\]", true)
explain select id from t order by l2_distance(v, '[1,2,3]') limit 3;
select id, round(l2_distance(v, '[1,2,3]'), 5) d from t order by l2_distance(v, '[1,2,3]') limit 3;
drop index ix on t;
create index ix using ivfflat on t(v) lists = 2 op_type 'vector_ip_ops';
-- @separator:table
-- @regex("Vector Index Scan on ix \[inner_product\]", true)
explain select id from t order by inner_product(v, '[1,2,3]') limit 3;
select id, round(inner_product(v, '[1,2,3]'), 5) d from t order by inner_product(v, '[1,2,3]') limit 3;
drop index ix on t;
create index ix using ivfflat on t(v) lists = 2 op_type 'vector_cosine_ops';
-- @separator:table
-- @regex("Vector Index Scan on ix \[cosine_distance\]", true)
explain select id from t order by cosine_distance(v, '[1,2,3]') limit 3;
select id, round(cosine_distance(v, '[1,2,3]'), 5) d from t order by cosine_distance(v, '[1,2,3]') limit 3;
-- @separator:table
-- @regex("Vector Index Scan", false)
explain select id from t order by cosine_similarity(v, '[1,2,3]') desc limit 3;
drop index ix on t;
create index ix using ivfflat on t(v) lists = 2 op_type 'vector_l1_ops';
-- @separator:table
-- @regex("Vector Index Scan on ix \[l1_distance\]", true)
explain select id from t order by l1_distance(v, '[1,2,3]') limit 3;
select id, round(l1_distance(v, '[1,2,3]'), 5) d from t order by l1_distance(v, '[1,2,3]') limit 3;
drop index ix on t;

create index ix using hnsw on t(v) M 8 EF_CONSTRUCTION 32 EF_SEARCH 32 op_type 'vector_l2_ops';
-- @separator:table
-- @regex("Vector Index Scan on ix \[l2_distance\]", true)
explain select id from t order by l2_distance(v, '[1,2,3]') limit 3;
select id, round(l2_distance(v, '[1,2,3]'), 5) d from t order by l2_distance(v, '[1,2,3]') limit 3;
-- @separator:table
-- @regex("Vector Index Scan on ix \[l2_distance_sq\]", true)
explain select id from t order by l2_distance_sq(v, '[1,2,3]') limit 3;
select id, round(l2_distance_sq(v, '[1,2,3]'), 5) d from t order by l2_distance_sq(v, '[1,2,3]') limit 3;
drop index ix on t;
create index ix using hnsw on t(v) M 8 EF_CONSTRUCTION 32 EF_SEARCH 32 op_type 'vector_ip_ops';
-- @separator:table
-- @regex("Vector Index Scan on ix \[inner_product\]", true)
explain select id from t order by inner_product(v, '[1,2,3]') limit 3;
select id, round(inner_product(v, '[1,2,3]'), 5) d from t order by inner_product(v, '[1,2,3]') limit 3;
drop index ix on t;
create index ix using hnsw on t(v) M 8 EF_CONSTRUCTION 32 EF_SEARCH 32 op_type 'vector_cosine_ops';
-- @separator:table
-- @regex("Vector Index Scan on ix \[cosine_distance\]", true)
explain select id from t order by cosine_distance(v, '[1,2,3]') limit 3;
select id, round(cosine_distance(v, '[1,2,3]'), 5) d from t order by cosine_distance(v, '[1,2,3]') limit 3;
select id from t order by cosine_distance(v, '[0,0,0]') limit 3;
drop index ix on t;
create index ix using hnsw on t(v) M 8 EF_CONSTRUCTION 32 EF_SEARCH 32 op_type 'vector_l1_ops';

-- int8 / uint8 quantization is L2-only (ip, cosine, l1 rejected); float16 / bf16 take every op_type
create index ixq using ivfflat on t(v) lists = 2 op_type 'vector_ip_ops' quantization 'int8';
create index ixq using ivfflat on t(v) lists = 2 op_type 'vector_cosine_ops' quantization 'uint8';
create index ixq using ivfflat on t(v) lists = 2 op_type 'vector_l1_ops' quantization 'int8';
create index ixq using hnsw on t(v) op_type 'vector_ip_ops' quantization 'int8';
create index ixq using hnsw on t(v) op_type 'vector_cosine_ops' quantization 'uint8';
create index ixq using ivfflat on t(v) lists = 2 op_type 'vector_l2sq_ops' quantization 'uint8';
drop index ixq on t;
create index ixq using ivfflat on t(v) lists = 2 op_type 'vector_l1_ops' quantization 'bf16';
drop index ixq on t;
create index ixq using hnsw on t(v) op_type 'vector_cosine_ops' quantization 'float16';
drop index ixq on t;
-- every column type builds IVF-FLAT and HNSW
create table tf64 (id bigint primary key, v vecf64(3));
create index ix using ivfflat on tf64(v) lists = 2 op_type 'vector_l2_ops';
create table tbf (id bigint primary key, v vecbf16(3));
create index ix using hnsw on tbf(v) op_type 'vector_l2_ops';
-- on a vecint8 / vecuint8 column IVF-FLAT serves every op_type and HNSW all but l1
create table t8 (id bigint primary key, v vecint8(3));
insert into t8 values (1,'[1,2,3]'),(2,'[-1,0,2]'),(3,'[3,1,-2]');
create index ix8 using hnsw on t8(v) M 8 EF_CONSTRUCTION 32 EF_SEARCH 32 op_type 'vector_ip_ops';
-- @separator:table
-- @regex("Vector Index Scan on ix8 \[inner_product\]", true)
explain select id from t8 order by inner_product(v, '[1,1,1]') limit 2;
select id, inner_product(v, '[1,1,1]') d from t8 order by inner_product(v, '[1,1,1]') limit 2;
drop index ix8 on t8;
create index ix8 using hnsw on t8(v) M 8 EF_CONSTRUCTION 32 EF_SEARCH 32 op_type 'vector_cosine_ops';
-- @separator:table
-- @regex("Vector Index Scan on ix8 \[cosine_distance\]", true)
explain select id from t8 order by cosine_distance(v, '[1,1,1]') limit 2;
drop index ix8 on t8;
create table u8 (id bigint primary key, v vecuint8(3));
insert into u8 values (1,'[1,2,3]'),(2,'[9,0,2]'),(3,'[3,1,7]');
create index ix8 using ivfflat on u8(v) lists = 2 op_type 'vector_l1_ops';
-- @separator:table
-- @regex("Vector Index Scan on ix8 \[l1_distance\]", true)
explain select id from u8 order by l1_distance(v, '[1,1,1]') limit 2;
select id, l1_distance(v, '[1,1,1]') d from u8 order by l1_distance(v, '[1,1,1]') limit 2;
drop index ix8 on u8;
create index ix8 using ivfflat on u8(v) lists = 2 op_type 'vector_cosine_ops';
-- @separator:table
-- @regex("Vector Index Scan on ix8 \[cosine_distance\]", true)
explain select id from u8 order by cosine_distance(v, '[1,1,1]') limit 2;

drop database vec_distance_semantics;
