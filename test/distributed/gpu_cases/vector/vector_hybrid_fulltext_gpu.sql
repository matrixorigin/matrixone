-- Hybrid fulltext + vector search (docs/design/20261008-index-search-scan.md).
-- A MATCH filter and a vector Top-K in one SELECT: ivfflat uses both the fulltext and
-- the vector index and post-filters its candidates, so a selective MATCH can return
-- fewer than k rows; hnsw, cagra and ivfpq use the fulltext index and sort its hits
-- exactly. A MATCH only in the projection is sorted exactly by every algorithm. Each EXPLAIN asserts which index scans the plan has. A subset claim counts
-- returned rows outside the exact matching set (expected 0). An exactness claim is
-- followed by the same query on t_ref_*, the same rows with only the fulltext index.
drop database if exists hybrid_ft_vec_gpu;
create database hybrid_ft_vec_gpu;
use hybrid_ft_vec_gpu;
set experimental_fulltext_index = 1;
set experimental_fulltext2_index = 1;
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

create table t_ft_cagra like src;
insert into t_ft_cagra select * from src;
create fulltext index f on t_ft_cagra(body);
create index vi using cagra on t_ft_cagra(v)  op_type 'vector_l2_ops';

create table t_ft2_cagra like src;
insert into t_ft2_cagra select * from src;
create fulltext2 index f on t_ft2_cagra(body) with parser ngram;
create index vi using cagra on t_ft2_cagra(v)  op_type 'vector_l2_ops';

create table t_ft_ivfpq like src;
insert into t_ft_ivfpq select * from src;
create fulltext index f on t_ft_ivfpq(body);
create index vi using ivfpq on t_ft_ivfpq(v) lists=2 m=3 op_type 'vector_l2_ops';

create table t_ft2_ivfpq like src;
insert into t_ft2_ivfpq select * from src;
create fulltext2 index f on t_ft2_ivfpq(body) with parser ngram;
create index vi using ivfpq on t_ft2_ivfpq(v) lists=2 m=3 op_type 'vector_l2_ops';

-- ---------------- ft + cagra ----------------
-- natural-language MATCH
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", false)
explain select id from t_ft_cagra where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
select id from t_ft_cagra where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
select id from t_ref_ft where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
-- boolean MATCH and a scalar filter
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", false)
explain select id from t_ft_cagra where match(body) against('+needle +anchor' in boolean mode) and tag in (1,2,3) order by l2_distance(v,'[0,0,0]') limit 3;
select id from t_ft_cagra where match(body) against('+needle +anchor' in boolean mode) and tag in (1,2,3) order by l2_distance(v,'[0,0,0]') limit 3;
select id from t_ref_ft where match(body) against('+needle +anchor' in boolean mode) and tag in (1,2,3) order by l2_distance(v,'[0,0,0]') limit 3;
-- far query vector
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", false)
explain select id from t_ft_cagra where match(body) against('needle') order by l2_distance(v,'[100,101,102]') limit 3;
select id from t_ft_cagra where match(body) against('needle') order by l2_distance(v,'[100,101,102]') limit 3;
select id from t_ref_ft where match(body) against('needle') order by l2_distance(v,'[100,101,102]') limit 3;
-- MATCH score projected
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", false)
explain select id, match(body) against('needle') as s from t_ft_cagra where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
select id, match(body) against('needle') as s from t_ft_cagra where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
select id, match(body) against('needle') as s from t_ref_ft where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
-- selective MATCH, 4 of 200 rows
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", false)
explain select id from t_ft_cagra where match(body) against('rare') order by l2_distance(v,'[0,0,0]') limit 3;
select id from t_ft_cagra where match(body) against('rare') order by l2_distance(v,'[0,0,0]') limit 3;
select id from t_ref_ft where match(body) against('rare') order by l2_distance(v,'[0,0,0]') limit 3;
-- MATCH only in the projection
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", false)
explain select id, match(body) against('needle') as s from t_ft_cagra order by l2_distance(v,'[0,0,0]') limit 3;
select id, match(body) against('needle') as s from t_ft_cagra order by l2_distance(v,'[0,0,0]') limit 3;
select id, match(body) against('needle') as s from t_ref_ft order by l2_distance(v,'[0,0,0]') limit 3;

-- ---------------- ft2 + cagra ----------------
-- natural-language MATCH
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", false)
explain select id from t_ft2_cagra where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
select id from t_ft2_cagra where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
select id from t_ref_ft2 where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
-- boolean MATCH and a scalar filter
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", false)
explain select id from t_ft2_cagra where match(body) against('+needle +anchor' in boolean mode) and tag in (1,2,3) order by l2_distance(v,'[0,0,0]') limit 3;
select id from t_ft2_cagra where match(body) against('+needle +anchor' in boolean mode) and tag in (1,2,3) order by l2_distance(v,'[0,0,0]') limit 3;
select id from t_ref_ft2 where match(body) against('+needle +anchor' in boolean mode) and tag in (1,2,3) order by l2_distance(v,'[0,0,0]') limit 3;
-- far query vector
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", false)
explain select id from t_ft2_cagra where match(body) against('needle') order by l2_distance(v,'[100,101,102]') limit 3;
select id from t_ft2_cagra where match(body) against('needle') order by l2_distance(v,'[100,101,102]') limit 3;
select id from t_ref_ft2 where match(body) against('needle') order by l2_distance(v,'[100,101,102]') limit 3;
-- MATCH score projected
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", false)
explain select id, match(body) against('needle') as s from t_ft2_cagra where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
select id, match(body) against('needle') as s from t_ft2_cagra where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
select id, match(body) against('needle') as s from t_ref_ft2 where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
-- selective MATCH, 4 of 200 rows
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", false)
explain select id from t_ft2_cagra where match(body) against('rare') order by l2_distance(v,'[0,0,0]') limit 3;
select id from t_ft2_cagra where match(body) against('rare') order by l2_distance(v,'[0,0,0]') limit 3;
select id from t_ref_ft2 where match(body) against('rare') order by l2_distance(v,'[0,0,0]') limit 3;
-- MATCH only in the projection
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", false)
explain select id, match(body) against('needle') as s from t_ft2_cagra order by l2_distance(v,'[0,0,0]') limit 3;
select id, match(body) against('needle') as s from t_ft2_cagra order by l2_distance(v,'[0,0,0]') limit 3;
select id, match(body) against('needle') as s from t_ref_ft2 order by l2_distance(v,'[0,0,0]') limit 3;

-- ---------------- ft + ivfpq ----------------
-- natural-language MATCH
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", false)
explain select id from t_ft_ivfpq where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
select id from t_ft_ivfpq where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
select id from t_ref_ft where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
-- boolean MATCH and a scalar filter
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", false)
explain select id from t_ft_ivfpq where match(body) against('+needle +anchor' in boolean mode) and tag in (1,2,3) order by l2_distance(v,'[0,0,0]') limit 3;
select id from t_ft_ivfpq where match(body) against('+needle +anchor' in boolean mode) and tag in (1,2,3) order by l2_distance(v,'[0,0,0]') limit 3;
select id from t_ref_ft where match(body) against('+needle +anchor' in boolean mode) and tag in (1,2,3) order by l2_distance(v,'[0,0,0]') limit 3;
-- far query vector
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", false)
explain select id from t_ft_ivfpq where match(body) against('needle') order by l2_distance(v,'[100,101,102]') limit 3;
select id from t_ft_ivfpq where match(body) against('needle') order by l2_distance(v,'[100,101,102]') limit 3;
select id from t_ref_ft where match(body) against('needle') order by l2_distance(v,'[100,101,102]') limit 3;
-- MATCH score projected
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", false)
explain select id, match(body) against('needle') as s from t_ft_ivfpq where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
select id, match(body) against('needle') as s from t_ft_ivfpq where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
select id, match(body) against('needle') as s from t_ref_ft where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
-- selective MATCH, 4 of 200 rows
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", false)
explain select id from t_ft_ivfpq where match(body) against('rare') order by l2_distance(v,'[0,0,0]') limit 3;
select id from t_ft_ivfpq where match(body) against('rare') order by l2_distance(v,'[0,0,0]') limit 3;
select id from t_ref_ft where match(body) against('rare') order by l2_distance(v,'[0,0,0]') limit 3;
-- MATCH only in the projection
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", false)
explain select id, match(body) against('needle') as s from t_ft_ivfpq order by l2_distance(v,'[0,0,0]') limit 3;
select id, match(body) against('needle') as s from t_ft_ivfpq order by l2_distance(v,'[0,0,0]') limit 3;
select id, match(body) against('needle') as s from t_ref_ft order by l2_distance(v,'[0,0,0]') limit 3;

-- ---------------- ft2 + ivfpq ----------------
-- natural-language MATCH
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", false)
explain select id from t_ft2_ivfpq where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
select id from t_ft2_ivfpq where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
select id from t_ref_ft2 where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
-- boolean MATCH and a scalar filter
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", false)
explain select id from t_ft2_ivfpq where match(body) against('+needle +anchor' in boolean mode) and tag in (1,2,3) order by l2_distance(v,'[0,0,0]') limit 3;
select id from t_ft2_ivfpq where match(body) against('+needle +anchor' in boolean mode) and tag in (1,2,3) order by l2_distance(v,'[0,0,0]') limit 3;
select id from t_ref_ft2 where match(body) against('+needle +anchor' in boolean mode) and tag in (1,2,3) order by l2_distance(v,'[0,0,0]') limit 3;
-- far query vector
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", false)
explain select id from t_ft2_ivfpq where match(body) against('needle') order by l2_distance(v,'[100,101,102]') limit 3;
select id from t_ft2_ivfpq where match(body) against('needle') order by l2_distance(v,'[100,101,102]') limit 3;
select id from t_ref_ft2 where match(body) against('needle') order by l2_distance(v,'[100,101,102]') limit 3;
-- MATCH score projected
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", false)
explain select id, match(body) against('needle') as s from t_ft2_ivfpq where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
select id, match(body) against('needle') as s from t_ft2_ivfpq where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
select id, match(body) against('needle') as s from t_ref_ft2 where match(body) against('needle') order by l2_distance(v,'[0,0,0]') limit 3;
-- selective MATCH, 4 of 200 rows
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", false)
explain select id from t_ft2_ivfpq where match(body) against('rare') order by l2_distance(v,'[0,0,0]') limit 3;
select id from t_ft2_ivfpq where match(body) against('rare') order by l2_distance(v,'[0,0,0]') limit 3;
select id from t_ref_ft2 where match(body) against('rare') order by l2_distance(v,'[0,0,0]') limit 3;
-- MATCH only in the projection
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", false)
explain select id, match(body) against('needle') as s from t_ft2_ivfpq order by l2_distance(v,'[0,0,0]') limit 3;
select id, match(body) against('needle') as s from t_ft2_ivfpq order by l2_distance(v,'[0,0,0]') limit 3;
select id, match(body) against('needle') as s from t_ref_ft2 order by l2_distance(v,'[0,0,0]') limit 3;

create table meta(id bigint primary key, grp int);
insert into meta select result, result % 2 from generate_series(1, 200) g;
create table query_vectors(name varchar(10) primary key, v vecf32(3) not null);
insert into query_vectors values ('lo', '[0,0,0]'), ('hi', '[100,101,102]');

-- Top-K over a join with another table: cagra uses no vector index, with or without a MATCH
-- @separator:table
-- @regex("Vector Index Scan on", false)
explain select d.id from t_ft_cagra d join meta m on m.id = d.id order by l2_distance(d.v,'[0,0,0]') limit 3;
select d.id from t_ft_cagra d join meta m on m.id = d.id order by l2_distance(d.v,'[0,0,0]') limit 3;
select d.id from t_ref_ft d join meta m on m.id = d.id order by l2_distance(d.v,'[0,0,0]') limit 3;
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", false)
explain select d.id from t_ft_cagra d join meta m on m.id = d.id where match(d.body) against('needle') order by l2_distance(d.v,'[0,0,0]') limit 3;
select d.id from t_ft_cagra d join meta m on m.id = d.id where match(d.body) against('needle') order by l2_distance(d.v,'[0,0,0]') limit 3;
select d.id from t_ref_ft d join meta m on m.id = d.id where match(d.body) against('needle') order by l2_distance(d.v,'[0,0,0]') limit 3;
-- query vector from a single-row provider: cagra uses no vector index, with or without a MATCH
-- @separator:table
-- @regex("Vector Index Scan on", false)
explain select d.id from t_ft_cagra d join query_vectors q on q.name = 'lo' order by l2_distance(d.v, q.v) limit 3;
select d.id from t_ft_cagra d join query_vectors q on q.name = 'lo' order by l2_distance(d.v, q.v) limit 3;
select d.id from t_ref_ft d join query_vectors q on q.name = 'lo' order by l2_distance(d.v, q.v) limit 3;
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", false)
explain select d.id from t_ft_cagra d join query_vectors q on q.name = 'hi' where match(d.body) against('needle') order by l2_distance(d.v, q.v) limit 3;
select d.id from t_ft_cagra d join query_vectors q on q.name = 'hi' where match(d.body) against('needle') order by l2_distance(d.v, q.v) limit 3;
select d.id from t_ref_ft d join query_vectors q on q.name = 'hi' where match(d.body) against('needle') order by l2_distance(d.v, q.v) limit 3;
-- rank mode clause: cagra plans and returns the same with mode=pre, mode=post and no clause
-- @separator:table
-- @regex("Join Type: SEMI", false)
-- @regex("Vector Index Scan on", true)
explain select id from t_ft_cagra where tag = 1 order by l2_distance(v,'[0,0,0]') limit 3 by rank with option 'mode=pre';
-- @separator:table
-- @regex("Join Type: SEMI", false)
-- @regex("Vector Index Scan on", true)
explain select id from t_ft_cagra where tag = 1 order by l2_distance(v,'[0,0,0]') limit 3 by rank with option 'mode=post';
select (select group_concat(id order by id) from (select id from t_ft_cagra where tag = 1 order by l2_distance(v,'[0,0,0]') limit 3 by rank with option 'mode=pre') x) = (select group_concat(id order by id) from (select id from t_ft_cagra where tag = 1 order by l2_distance(v,'[0,0,0]') limit 3) x) as same_pre;
select (select group_concat(id order by id) from (select id from t_ft_cagra where tag = 1 order by l2_distance(v,'[0,0,0]') limit 3 by rank with option 'mode=post') x) = (select group_concat(id order by id) from (select id from t_ft_cagra where tag = 1 order by l2_distance(v,'[0,0,0]') limit 3) x) as same_post;

-- Top-K over a join with another table: ivfpq uses no vector index, with or without a MATCH
-- @separator:table
-- @regex("Vector Index Scan on", false)
explain select d.id from t_ft_ivfpq d join meta m on m.id = d.id order by l2_distance(d.v,'[0,0,0]') limit 3;
select d.id from t_ft_ivfpq d join meta m on m.id = d.id order by l2_distance(d.v,'[0,0,0]') limit 3;
select d.id from t_ref_ft d join meta m on m.id = d.id order by l2_distance(d.v,'[0,0,0]') limit 3;
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", false)
explain select d.id from t_ft_ivfpq d join meta m on m.id = d.id where match(d.body) against('needle') order by l2_distance(d.v,'[0,0,0]') limit 3;
select d.id from t_ft_ivfpq d join meta m on m.id = d.id where match(d.body) against('needle') order by l2_distance(d.v,'[0,0,0]') limit 3;
select d.id from t_ref_ft d join meta m on m.id = d.id where match(d.body) against('needle') order by l2_distance(d.v,'[0,0,0]') limit 3;
-- query vector from a single-row provider: ivfpq uses no vector index, with or without a MATCH
-- @separator:table
-- @regex("Vector Index Scan on", false)
explain select d.id from t_ft_ivfpq d join query_vectors q on q.name = 'lo' order by l2_distance(d.v, q.v) limit 3;
select d.id from t_ft_ivfpq d join query_vectors q on q.name = 'lo' order by l2_distance(d.v, q.v) limit 3;
select d.id from t_ref_ft d join query_vectors q on q.name = 'lo' order by l2_distance(d.v, q.v) limit 3;
-- @separator:table
-- @regex("Fulltext Index Scan on", true)
-- @regex("Vector Index Scan on", false)
explain select d.id from t_ft_ivfpq d join query_vectors q on q.name = 'hi' where match(d.body) against('needle') order by l2_distance(d.v, q.v) limit 3;
select d.id from t_ft_ivfpq d join query_vectors q on q.name = 'hi' where match(d.body) against('needle') order by l2_distance(d.v, q.v) limit 3;
select d.id from t_ref_ft d join query_vectors q on q.name = 'hi' where match(d.body) against('needle') order by l2_distance(d.v, q.v) limit 3;
-- rank mode clause: ivfpq plans and returns the same with mode=pre, mode=post and no clause
-- @separator:table
-- @regex("Join Type: SEMI", false)
-- @regex("Vector Index Scan on", true)
explain select id from t_ft_ivfpq where tag = 1 order by l2_distance(v,'[0,0,0]') limit 3 by rank with option 'mode=pre';
-- @separator:table
-- @regex("Join Type: SEMI", false)
-- @regex("Vector Index Scan on", true)
explain select id from t_ft_ivfpq where tag = 1 order by l2_distance(v,'[0,0,0]') limit 3 by rank with option 'mode=post';
select (select group_concat(id order by id) from (select id from t_ft_ivfpq where tag = 1 order by l2_distance(v,'[0,0,0]') limit 3 by rank with option 'mode=pre') x) = (select group_concat(id order by id) from (select id from t_ft_ivfpq where tag = 1 order by l2_distance(v,'[0,0,0]') limit 3) x) as same_pre;
select (select group_concat(id order by id) from (select id from t_ft_ivfpq where tag = 1 order by l2_distance(v,'[0,0,0]') limit 3 by rank with option 'mode=post') x) = (select group_concat(id order by id) from (select id from t_ft_ivfpq where tag = 1 order by l2_distance(v,'[0,0,0]') limit 3) x) as same_post;

drop database hybrid_ft_vec_gpu;
set experimental_fulltext_index = 0;
set experimental_fulltext2_index = 0;
set experimental_cagra_index = 0;
set experimental_ivfpq_index = 0;
