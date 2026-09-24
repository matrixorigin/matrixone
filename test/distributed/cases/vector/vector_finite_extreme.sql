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

-- A cosine kernel must reject its own squared norm when it lands in the subnormals, not only when
-- it reaches zero or Inf. Each element of [3e-23,3e-23] squares into the float32 subnormals, so
-- the accumulated norm keeps a couple of bits; the denominator built from it is positive and
-- finite, and the kernel answered 0.433316 where the cosine distance is 0.292893.
create table tsub(id int primary key, v vecf32(2));
insert into tsub values (1, '[3e-23,3e-23]'), (2, '[3,4]');
select id, cosine_distance(v, '[3e-23,0]') from tsub order by id;

-- normalize_l2 rejects a vector whose squared norm overflows or underflows the float64 domain,
-- and keeps copying an all-zero vector unchanged.
select normalize_l2(v) from t64 order by id;
create table tsmall(id int primary key, v vecf64(2));
insert into tsmall values (1, '[1e-300,1e-300]'), (2, '[0,0]'), (3, '[3,4]');
select normalize_l2(v) from tsmall where id = 1;
select id, normalize_l2(v) from tsmall where id in (2,3) order by id;
-- A squared norm that lands in the subnormals is rejected too: it is still positive and finite, so
-- a zero/Inf/NaN test accepts it, but it keeps far too few bits and [2e-162] normalized to
-- 0.899783 instead of 1. The smallest magnitude whose square stays normal is still accepted.
insert into tsmall values (4, '[2e-162,0]'), (5, '[1.5e-154,0]');
select normalize_l2(v) from tsmall where id = 4;
select id, normalize_l2(v) from tsmall where id = 5;

-- inner_product rejects a dot product that leaves the element domain: each term is finite, the
-- sum is not. Constant and column operands report the same error.
create table tip(id int primary key, v vecf32(2));
insert into tip values (1, '[1e20,1e20]'), (2, '[1,2]');
select id, inner_product(v, '[1e20,-1e20]') from tip order by id;
select a.id, inner_product(a.v, b.v) from tip a join tip b on b.id = 1 where a.id = 1;
select id, inner_product(v, '[1,1]') from tip where id = 2;

-- A float64 vector whose squared norm underflows has no computable cosine; it is rejected rather
-- than reported as maximally dissimilar (distance 1) against itself.
create table tu(id int primary key, v vecf64(2));
insert into tu values (1, '[1e-200,0]'), (2, '[3,4]');
select cosine_distance(v, '[1e-200,0]') from tu order by id;
select cosine_similarity(v, '[1e-200,0]') from tu order by id;
-- A genuinely zero vector keeps its convention.
select cosine_distance(v, '[0,0]') from tu where id = 2;

-- l2_distance_xc / l2_distance_sq_xc are a second, C implementation of the same distance. They
-- compute the element difference and its square in float32 and report success whatever comes out,
-- so they must be held to the same contract as l2_distance where the value crosses back into Go.
select l2_distance_xc(cast('[2e19,2e19]' as vecf32(2)), cast('[0,0]' as vecf32(2)));
select l2_distance_sq_xc(cast('[2e19,2e19]' as vecf32(2)), cast('[0,0]' as vecf32(2)));
-- ordinary magnitudes agree with the Go path
select l2_distance(cast('[3,4]' as vecf32(2)), cast('[0,0]' as vecf32(2))) as go_path,
       l2_distance_xc(cast('[3,4]' as vecf32(2)), cast('[0,0]' as vecf32(2))) as c_path,
       l2_distance_sq_xc(cast('[3,4]' as vecf32(2)), cast('[0,0]' as vecf32(2))) as c_sq;
-- The vecf64 overload accumulates in double, so its result can be a finite float64 that is outside
-- the float32 domain l2_distance delivers for every base type. Both must reject it. l2_distance_sq
-- on vecf64 is the exception: it returns the raw float64 square by design, as IVF's squared
-- intermediate, so 1e80 is in domain there.
select l2_distance(cast('[1e40,0]' as vecf64(2)), cast('[0,0]' as vecf64(2)));
select l2_distance_xc(cast('[1e40,0]' as vecf64(2)), cast('[0,0]' as vecf64(2)));
select l2_distance_sq(cast('[1e40,0]' as vecf64(2)), cast('[0,0]' as vecf64(2)));
select l2_distance_sq_xc(cast('[1e40,0]' as vecf64(2)), cast('[0,0]' as vecf64(2)));
select l2_distance(cast('[3,4]' as vecf64(2)), cast('[0,0]' as vecf64(2))) as go_path,
       l2_distance_xc(cast('[3,4]' as vecf64(2)), cast('[0,0]' as vecf64(2))) as c_path;

drop database vec_extreme;
