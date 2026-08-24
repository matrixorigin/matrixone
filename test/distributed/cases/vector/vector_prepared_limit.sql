-- A prepared statement with a parameterized LIMIT on a filtered vector-index
-- top-k must return the same rows as the literal form. Before the fix the
-- post-filter search under-fetched candidates: the residual filter (id >= ?)
-- dropped index candidates and too few rows survived (issues #26878 ivfflat,
-- #26869 hnsw). The candidate over-fetch is now resolved at EXECUTE.
--
-- Data: 9 collinear points [k,k,k]. Query [2.1,2.1,2.1] nearest order is
-- 3,4,2,5,... ; with id >= 4 the nearest surviving rows are 4,5,6.

-- ================= ivfflat =================
SET experimental_ivf_index = 1;
drop database if exists ivf_prepared_limit;
create database ivf_prepared_limit;
use ivf_prepared_limit;
create table t(id bigint primary key, v vecf32(3));
insert into t values (1,'[0,0,0]'),(2,'[1,1,1]'),(3,'[2,2,2]'),(4,'[3,3,3]'),(5,'[4,4,4]'),(6,'[5,5,5]'),(7,'[6,6,6]'),(8,'[7,7,7]'),(9,'[8,8,8]');
create index ix using ivfflat on t(v) lists=1 op_type 'vector_l2_ops';
-- @separator:table
-- @regex("Vector Index Scan", true)
explain select id from t where id >= 4 order by l2_distance(v,'[2.1,2.1,2.1]') limit 2;
select id from t where id >= 4 order by l2_distance(v,'[2.1,2.1,2.1]') limit 2;
prepare s from 'select id from t where id >= ? order by l2_distance(v, cast(? as vecf32(3))) limit ?';
set @lo = 4;
set @q = '[2.1,2.1,2.1]';
set @k = 2;
execute s using @lo, @q, @k;
set @k = 3;
execute s using @lo, @q, @k;
deallocate prepare s;

-- A correlated vector provider lowers the scan beneath APPLY. Its covered
-- pre-filter must be rebound from the immutable template on every EXECUTE.
create table tc(id bigint primary key, v vecf32(2), category varchar(10) not null);
create table query_vector(name varchar(10) primary key, v vecf32(2) not null);
insert into tc values
    (1,'[0,0]','x'),(2,'[1,0]','x'),(3,'[0,1]','y'),(4,'[2,0]','y');
insert into query_vector values ('ref','[0,0]');
create index ix_apply using ivfflat on tc(v) lists=1 op_type 'vector_l2_ops' include(category);
-- @separator:table
-- @regex("Vector Index Scan", true)
explain select tc.id from tc, query_vector q
where q.name = 'ref' and tc.category = concat('x', '')
order by l2_distance(tc.v, q.v) limit 2 by rank with option 'mode=include';
prepare s_apply from 'select tc.id from tc, query_vector q where q.name = ''ref'' and tc.category = concat(?, '''') order by l2_distance(tc.v, q.v) limit 2 by rank with option ''mode=include''';
set @category = 'x';
execute s_apply using @category;
set @category = 'y';
execute s_apply using @category;
deallocate prepare s_apply;
drop database ivf_prepared_limit;
SET experimental_ivf_index = 0;

-- ================= hnsw =================
SET experimental_hnsw_index = 1;
drop database if exists hnsw_prepared_limit;
create database hnsw_prepared_limit;
use hnsw_prepared_limit;
create table t(id bigint primary key, v vecf32(3));
insert into t values (1,'[0,0,0]'),(2,'[1,1,1]'),(3,'[2,2,2]'),(4,'[3,3,3]'),(5,'[4,4,4]'),(6,'[5,5,5]'),(7,'[6,6,6]'),(8,'[7,7,7]'),(9,'[8,8,8]');
create index hidx using hnsw on t(v) op_type 'vector_l2_ops' M 4 EF_CONSTRUCTION 16 EF_SEARCH 16;
select id from t where id >= 4 order by l2_distance(v,'[2.1,2.1,2.1]') limit 2;
prepare s from 'select id from t where id >= ? order by l2_distance(v, cast(? as vecf32(3))) limit ?';
set @lo = 4;
set @q = '[2.1,2.1,2.1]';
set @k = 2;
execute s using @lo, @q, @k;
set @k = 3;
execute s using @lo, @q, @k;
deallocate prepare s;
drop database hnsw_prepared_limit;
SET experimental_hnsw_index = 0;
