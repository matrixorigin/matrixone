-- Finite-extreme boundaries of the vector distance functions. Whichever kernel runs (the batch
-- kernel is selected when exactly one operand is constant and the column has no nulls), a query
-- answers with the same value or the same error: an intermediate that leaves the element domain is
-- rejected rather than returned as Inf / NaN / silently wrong.
drop database if exists vec_extreme;
create database vec_extreme;
use vec_extreme;

create table t32(id int primary key, v vecf32(2));
insert into t32 values (1, '[2e19,2e19]'), (2, '[3,4]');

-- l2_distance: the square overflows float32 although the distance itself is representable. Every
-- kernel accumulates that square in float32, so none can answer the pair and all reject it -- the
-- constant-vs-column (batch) form, the column-vs-column (per-row) form, and the NULL-in-column form
-- that disables batching alike. Making an operand constant must not change what SQL returns.
select id, l2_distance(v, '[0,0]') from t32 order by id;
select a.id, l2_distance(a.v, b.v) from t32 a join t32 b on b.id = 2 order by a.id;
create table t32n(id int primary key, v vecf32(2));
insert into t32n values (1, '[2e19,2e19]'), (2, null);
select id, l2_distance(v, '[0,0]') from t32n order by id;

-- Ordinary magnitudes are unaffected on every form.
select id, l2_distance(v, '[0,0]') from t32 where id = 2;
select a.id, l2_distance(a.v, b.v) from t32 a join t32 b on b.id = 2 where a.id = 2;
select l2_distance(v, '[0,0]') = l2_distance('[0,0]', v) as symmetric from t32 where id = 2;

-- cosine_similarity rejects a zero-magnitude vector on both the batch and the per-row path.
create table tz(id int primary key, v vecf32(2));
insert into tz values (1, '[0,0]'), (2, '[1,1]');
select cosine_similarity(v, '[1,1]') from tz order by id;
select cosine_similarity(v, v) from tz order by id;
-- cosine_distance keeps its zero-vector convention (1) on both paths.
select id, cosine_distance(v, '[1,1]') from tz order by id;

-- A float64 vector whose squared magnitude leaves the float64 domain has no computable cosine.
create table t64(id int primary key, v vecf64(2));
insert into t64 values (1, '[1e200,1e200]'), (2, '[3,4]');
select cosine_distance(v, '[1e200,1e200]') from t64 order by id;
select cosine_similarity(v, '[1e200,1e200]') from t64 order by id;
select id, cosine_distance(v, '[3,4]') from t64 where id = 2;

-- normalize_l2 rejects a vector whose squared norm overflows or underflows the float64 domain,
-- and keeps copying an all-zero vector unchanged.
select normalize_l2(v) from t64 order by id;
create table tsmall(id int primary key, v vecf64(2));
insert into tsmall values (1, '[1e-300,1e-300]'), (2, '[0,0]'), (3, '[3,4]');
select normalize_l2(v) from tsmall where id = 1;
select id, normalize_l2(v) from tsmall where id in (2,3) order by id;

drop database vec_extreme;
