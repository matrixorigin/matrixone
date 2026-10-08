-- Hybrid fulltext + vector search (docs/design/20261008-index-search-scan.md).
-- A MATCH filter and a vector Top-K in one SELECT use both the fulltext index and
-- the vector index; the vector side post-filters its candidates, so the result is a
-- subset of the exact answer and may hold fewer than k rows.
-- Each EXPLAIN asserts both index scans. A subset claim counts returned rows outside
-- the exact matching set (expected 0). An exactness claim is followed by the same
-- query on t_ref_*, the same rows with only the fulltext index.
drop database if exists hybrid_ft_vec;
create database hybrid_ft_vec;
use hybrid_ft_vec;
set experimental_fulltext_index = 1;
set experimental_fulltext2_index = 1;
set experimental_hnsw_index = 1;
set experimental_ivf_index = 1;
set experimental_cagra_index = 1;
set experimental_ivfpq_index = 1;

create table src(id bigint primary key, body text, tag int, v vecf32(3) not null);
insert into src select result,
    case when result % 50 = 0 then 'needle anchor rare text'
         when result % 3 = 0 then 'needle anchor text'
         else 'plain text' end,
    result % 7,
    concat('[', result, ',', result + 1, ',', result + 2, ']')
from generate_series(1, 200) g;

create table t_ref_ft like src;
insert into t_ref_ft select * from src;
create fulltext index f on t_ref_ft(body);

create table t_ref_ft2 like src;
insert into t_ref_ft2 select * from src;
create fulltext2 index f on t_ref_ft2(body) with parser ngram;

create table t_ft_ivfflat like src;
insert into t_ft_ivfflat select * from src;
create fulltext index f on t_ft_ivfflat(body);
create index vi using ivfflat on t_ft_ivfflat(v) lists=4 op_type 'vector_l2_ops';

create table t_ft2_ivfflat like src;
insert into t_ft2_ivfflat select * from src;
create fulltext2 index f on t_ft2_ivfflat(body) with parser ngram;
create index vi using ivfflat on t_ft2_ivfflat(v) lists=4 op_type 'vector_l2_ops';

create table t_ft_hnsw like src;
insert into t_ft_hnsw select * from src;
create fulltext index f on t_ft_hnsw(body);
create index vi using hnsw on t_ft_hnsw(v)  op_type 'vector_l2_ops';

create table t_ft2_hnsw like src;
insert into t_ft2_hnsw select * from src;
create fulltext2 index f on t_ft2_hnsw(body) with parser ngram;
create index vi using hnsw on t_ft2_hnsw(v)  op_type 'vector_l2_ops';

-- ---------------- ft + ivfflat ----------------
-- natural-language MATCH
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", true)
explain select id from t_ft_ivfflat where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
select id from t_ft_ivfflat where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
select id from t_ref_ft where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
-- boolean MATCH and a scalar filter
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", true)
explain select id from t_ft_ivfflat where match(body) against('+needle +anchor' in boolean mode) and tag in (1,2,3) order by l2_distance(v,'[0,0,0]') limit 3;
select id from t_ft_ivfflat where match(body) against('+needle +anchor' in boolean mode) and tag in (1,2,3) order by l2_distance(v,'[0,0,0]') limit 3;
select id from t_ref_ft where match(body) against('+needle +anchor' in boolean mode) and tag in (1,2,3) order by l2_distance(v,'[0,0,0]') limit 3;
-- far query vector
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", true)
explain select id from t_ft_ivfflat where match(body) against('needle') order by l2_distance(v,'[100,101,102]') limit 3;
select id from t_ft_ivfflat where match(body) against('needle') order by l2_distance(v,'[100,101,102]') limit 3;
select id from t_ref_ft where match(body) against('needle') order by l2_distance(v,'[100,101,102]') limit 3;
-- MATCH score projected
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", true)
explain select id, match(body) against('needle') as s from t_ft_ivfflat where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
select id, match(body) against('needle') as s from t_ft_ivfflat where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
select id, match(body) against('needle') as s from t_ref_ft where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
-- selective MATCH, 4 of 200 rows
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", true)
explain select id from t_ft_ivfflat where match(body) against('rare') order by l2_distance(v,'[0,0,0]') limit 3;
select count(*) as outside_exact from (select id from t_ft_ivfflat where match(body) against('rare') order by l2_distance(v,'[0,0,0]') limit 3) x where x.id not in (select id from t_ref_ft where match(body) against('rare'));

-- ---------------- ft2 + ivfflat ----------------
-- natural-language MATCH
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", true)
explain select id from t_ft2_ivfflat where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
select id from t_ft2_ivfflat where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
select id from t_ref_ft2 where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
-- boolean MATCH and a scalar filter
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", true)
explain select id from t_ft2_ivfflat where match(body) against('+needle +anchor' in boolean mode) and tag in (1,2,3) order by l2_distance(v,'[0,0,0]') limit 3;
select id from t_ft2_ivfflat where match(body) against('+needle +anchor' in boolean mode) and tag in (1,2,3) order by l2_distance(v,'[0,0,0]') limit 3;
select id from t_ref_ft2 where match(body) against('+needle +anchor' in boolean mode) and tag in (1,2,3) order by l2_distance(v,'[0,0,0]') limit 3;
-- far query vector
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", true)
explain select id from t_ft2_ivfflat where match(body) against('needle') order by l2_distance(v,'[100,101,102]') limit 3;
select id from t_ft2_ivfflat where match(body) against('needle') order by l2_distance(v,'[100,101,102]') limit 3;
select id from t_ref_ft2 where match(body) against('needle') order by l2_distance(v,'[100,101,102]') limit 3;
-- MATCH score projected
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", true)
explain select id, match(body) against('needle') as s from t_ft2_ivfflat where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
select id, match(body) against('needle') as s from t_ft2_ivfflat where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
select id, match(body) against('needle') as s from t_ref_ft2 where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
-- selective MATCH, 4 of 200 rows
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", true)
explain select id from t_ft2_ivfflat where match(body) against('rare') order by l2_distance(v,'[0,0,0]') limit 3;
select count(*) as outside_exact from (select id from t_ft2_ivfflat where match(body) against('rare') order by l2_distance(v,'[0,0,0]') limit 3) x where x.id not in (select id from t_ref_ft2 where match(body) against('rare'));

-- ---------------- ft + hnsw ----------------
-- natural-language MATCH
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", true)
explain select id from t_ft_hnsw where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
select id from t_ft_hnsw where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
select id from t_ref_ft where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
-- boolean MATCH and a scalar filter
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", true)
explain select id from t_ft_hnsw where match(body) against('+needle +anchor' in boolean mode) and tag in (1,2,3) order by l2_distance(v,'[0,0,0]') limit 3;
select id from t_ft_hnsw where match(body) against('+needle +anchor' in boolean mode) and tag in (1,2,3) order by l2_distance(v,'[0,0,0]') limit 3;
select id from t_ref_ft where match(body) against('+needle +anchor' in boolean mode) and tag in (1,2,3) order by l2_distance(v,'[0,0,0]') limit 3;
-- far query vector
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", true)
explain select id from t_ft_hnsw where match(body) against('needle') order by l2_distance(v,'[100,101,102]') limit 3;
select id from t_ft_hnsw where match(body) against('needle') order by l2_distance(v,'[100,101,102]') limit 3;
select id from t_ref_ft where match(body) against('needle') order by l2_distance(v,'[100,101,102]') limit 3;
-- MATCH score projected
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", true)
explain select id, match(body) against('needle') as s from t_ft_hnsw where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
select id, match(body) against('needle') as s from t_ft_hnsw where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
select id, match(body) against('needle') as s from t_ref_ft where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
-- selective MATCH, 4 of 200 rows
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", true)
explain select id from t_ft_hnsw where match(body) against('rare') order by l2_distance(v,'[0,0,0]') limit 3;
select count(*) as outside_exact from (select id from t_ft_hnsw where match(body) against('rare') order by l2_distance(v,'[0,0,0]') limit 3) x where x.id not in (select id from t_ref_ft where match(body) against('rare'));

-- ---------------- ft2 + hnsw ----------------
-- natural-language MATCH
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", true)
explain select id from t_ft2_hnsw where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
select id from t_ft2_hnsw where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
select id from t_ref_ft2 where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
-- boolean MATCH and a scalar filter
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", true)
explain select id from t_ft2_hnsw where match(body) against('+needle +anchor' in boolean mode) and tag in (1,2,3) order by l2_distance(v,'[0,0,0]') limit 3;
select id from t_ft2_hnsw where match(body) against('+needle +anchor' in boolean mode) and tag in (1,2,3) order by l2_distance(v,'[0,0,0]') limit 3;
select id from t_ref_ft2 where match(body) against('+needle +anchor' in boolean mode) and tag in (1,2,3) order by l2_distance(v,'[0,0,0]') limit 3;
-- far query vector
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", true)
explain select id from t_ft2_hnsw where match(body) against('needle') order by l2_distance(v,'[100,101,102]') limit 3;
select id from t_ft2_hnsw where match(body) against('needle') order by l2_distance(v,'[100,101,102]') limit 3;
select id from t_ref_ft2 where match(body) against('needle') order by l2_distance(v,'[100,101,102]') limit 3;
-- MATCH score projected
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", true)
explain select id, match(body) against('needle') as s from t_ft2_hnsw where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
select id, match(body) against('needle') as s from t_ft2_hnsw where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
select id, match(body) against('needle') as s from t_ref_ft2 where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
-- selective MATCH, 4 of 200 rows
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", true)
explain select id from t_ft2_hnsw where match(body) against('rare') order by l2_distance(v,'[0,0,0]') limit 3;
select count(*) as outside_exact from (select id from t_ft2_hnsw where match(body) against('rare') order by l2_distance(v,'[0,0,0]') limit 3) x where x.id not in (select id from t_ref_ft2 where match(body) against('rare'));

create table query_vectors(name varchar(10) primary key, v vecf32(3) not null);
insert into query_vectors values ('lo', '[0,0,0]'), ('hi', '[100,101,102]');

-- query vector from a single-row provider, ft + ivfflat
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", true)
explain select d.id from t_ft_ivfflat d join query_vectors q on q.name = 'hi' where match(d.body) against('needle') order by l2_distance(d.v, q.v) limit 3;
select d.id from t_ft_ivfflat d join query_vectors q on q.name = 'hi' where match(d.body) against('needle') order by l2_distance(d.v, q.v) limit 3;
select d.id from t_ref_ft d join query_vectors q on q.name = 'hi' where match(d.body) against('needle') order by l2_distance(d.v, q.v) limit 3;
-- query vector from a single-row provider, ft2 + ivfflat
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", true)
explain select d.id from t_ft2_ivfflat d join query_vectors q on q.name = 'hi' where match(d.body) against('needle') order by l2_distance(d.v, q.v) limit 3;
select d.id from t_ft2_ivfflat d join query_vectors q on q.name = 'hi' where match(d.body) against('needle') order by l2_distance(d.v, q.v) limit 3;
select d.id from t_ref_ft2 d join query_vectors q on q.name = 'hi' where match(d.body) against('needle') order by l2_distance(d.v, q.v) limit 3;
-- query vector from a single-row provider, ft + hnsw
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", true)
explain select d.id from t_ft_hnsw d join query_vectors q on q.name = 'hi' where match(d.body) against('needle') order by l2_distance(d.v, q.v) limit 3;
select d.id from t_ft_hnsw d join query_vectors q on q.name = 'hi' where match(d.body) against('needle') order by l2_distance(d.v, q.v) limit 3;
select d.id from t_ref_ft d join query_vectors q on q.name = 'hi' where match(d.body) against('needle') order by l2_distance(d.v, q.v) limit 3;
-- query vector from a single-row provider, ft2 + hnsw
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", true)
explain select d.id from t_ft2_hnsw d join query_vectors q on q.name = 'hi' where match(d.body) against('needle') order by l2_distance(d.v, q.v) limit 3;
select d.id from t_ft2_hnsw d join query_vectors q on q.name = 'hi' where match(d.body) against('needle') order by l2_distance(d.v, q.v) limit 3;
select d.id from t_ref_ft2 d join query_vectors q on q.name = 'hi' where match(d.body) against('needle') order by l2_distance(d.v, q.v) limit 3;

-- the search table functions are not callable from SQL
select * from hnsw_search('{}', '{}', '[0,0,0]');
select * from ivfpq_search('{}', '{}', '[0,0,0]');
select * from cagra_search('{}', '{}', '[0,0,0]');
select * from fulltext2_search('{}', '{}', 'needle', 0);
select * from fulltext_index_scan('{}', 'src', 'src', 'needle', 0);

drop database hybrid_ft_vec;
