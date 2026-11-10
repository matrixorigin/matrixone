-- Regression for #30164: preserve results when implicit fulltext ordering is overwritten.
-- Eight source rows per owner, three immutable index builds, one database/session.
-- No ANN index is needed: exact vector sorting of fulltext hits is the contract.
set @hybrid_old_ft = @@ft_relevancy_algorithm;
set @hybrid_old_ft2 = @@ft2_relevancy_algorithm;
set @hybrid_old_exp_ft = @@experimental_fulltext_index;
set @hybrid_old_exp_ft2 = @@experimental_fulltext2_index;
set @hybrid_old_dop = @@max_dop;
set ft_relevancy_algorithm = 'TF-IDF';
set ft2_relevancy_algorithm = 'TF-IDF';
set experimental_fulltext_index = 1;
set experimental_fulltext2_index = 1;
set max_dop = 1;
drop database if exists mo30164_validation;
create database mo30164_validation;
use mo30164_validation;
create table classic (
  id bigint primary key,
  body text not null,
  prio int not null,
  v vecf32(2) not null
);
insert into classic values
  (1, 'needle needle needle needle', 90, '[90,0]'),
  (2, 'needle needle needle',        80, '[80,0]'),
  (3, 'needle needle',               70, '[70,0]'),
  (4, 'needle alpha',                30, '[3,0]'),
  (5, 'needle alpha',                10, '[1,0]'),
  (6, 'needle alpha',                10, '[-1,0]'),
  (7, 'other alpha',                  0, '[0,0]'),
  (8, 'other beta',                   0, '[0,0]');
-- Copy before CREATE INDEX, so LIKE cannot accidentally copy any index.
create table ft2 like classic;
create table covered like classic;
insert into ft2 select * from classic;
insert into covered select * from classic;
create fulltext index classic_ft on classic(body) with parser gojieba;
create fulltext2 index ft2_ft on ft2(body) with parser gojieba;
-- VECF32 is not an accepted INCLUDE type on this revision. Covered tests use prio.
create fulltext2 index covered_ft on covered(body) include(prio) with parser gojieba;

-- classic/all_candidates
select id from classic where match(body) against('needle' in natural language mode) order by id;

-- classic/nonvector_multiple_keys
select id from classic where match(body) against('needle' in natural language mode) order by prio, id desc limit 2 offset 1;

-- classic/projected_score
select id, cast(match(body) against('needle' in natural language mode) as decimal(10,3)) as score from classic where match(body) against('needle' in natural language mode) order by id desc;

-- classic/real_score_threshold
select id from classic where match(body) against('needle' in natural language mode) > 0.04 order by prio, id desc;

-- classic/default_relevance_top2
select id from classic where match(body) against('needle' in natural language mode) limit 2;

-- classic/explicit_relevance_ties
select id from classic where match(body) against('needle' in natural language mode) order by match(body) against('needle' in natural language mode) desc, id desc;

-- classic/nested_relevance_page_barrier
select id from (select id, prio from classic where match(body) against('needle' in natural language mode) limit 2 offset 1) as page order by prio, id;

-- classic/vector_single_key
select id from classic where match(body) against('needle' in natural language mode) order by l2_distance(v, '[0,0]') limit 2 offset 2;

-- classic/vector_multiple_keys_tie
select id from classic where match(body) against('needle' in natural language mode) order by l2_distance(v, '[0,0]'), id desc limit 2 offset 1;

-- ft2/all_candidates
select id from ft2 where match(body) against('needle' in natural language mode) order by id;

-- ft2/nonvector_multiple_keys
select id from ft2 where match(body) against('needle' in natural language mode) order by prio, id desc limit 2 offset 1;

-- ft2/projected_score
select id, cast(match(body) against('needle' in natural language mode) as decimal(10,3)) as score from ft2 where match(body) against('needle' in natural language mode) order by id desc;

-- ft2/real_score_threshold
select id from ft2 where match(body) against('needle' in natural language mode) > 0.04 order by prio, id desc;

-- ft2/default_relevance_top2
select id from ft2 where match(body) against('needle' in natural language mode) limit 2;

-- ft2/explicit_relevance_ties
select id from ft2 where match(body) against('needle' in natural language mode) order by match(body) against('needle' in natural language mode) desc, id desc;

-- ft2/nested_relevance_page_barrier
select id from (select id, prio from ft2 where match(body) against('needle' in natural language mode) limit 2 offset 1) as page order by prio, id;

-- ft2/vector_single_key
select id from ft2 where match(body) against('needle' in natural language mode) order by l2_distance(v, '[0,0]') limit 2 offset 2;

-- ft2/vector_multiple_keys_tie
select id from ft2 where match(body) against('needle' in natural language mode) order by l2_distance(v, '[0,0]'), id desc limit 2 offset 1;

-- covered/all_candidates
select id from covered where match(body) against('needle' in natural language mode) order by id;

-- covered/nonvector_multiple_keys
select id from covered where match(body) against('needle' in natural language mode) order by prio, id desc limit 2 offset 1;

-- covered/projected_score
select id, cast(match(body) against('needle' in natural language mode) as decimal(10,3)) as score from covered where match(body) against('needle' in natural language mode) order by id desc;

-- covered/real_score_threshold
select id from covered where match(body) against('needle' in natural language mode) > 0.04 order by prio, id desc;

-- covered/default_relevance_top2
select id from covered where match(body) against('needle' in natural language mode) limit 2;

-- covered/explicit_relevance_ties
select id from covered where match(body) against('needle' in natural language mode) order by match(body) against('needle' in natural language mode) desc, id desc;

-- covered/nested_relevance_page_barrier
select id from (select id, prio from covered where match(body) against('needle' in natural language mode) limit 2 offset 1) as page order by prio, id;

-- Python handlers have no purity declaration. Binding and EXPLAIN need no
-- Python service, but must preserve relevance order before an order-sensitive
-- UDF projection. The projected alias is reused so the projection stays below
-- the explicit sort; check classic, ordinary FULLTEXT2 and covered FULLTEXT2.
create function order_tick(x bigint) returns bigint language python as 'def tick(x, counter=[0]): counter[0] += 1; return counter[0]' handler 'tick';
-- @regex("(?s)Sort Key:.*Sort Key:",true)
explain select id, order_tick(id) as seq from classic where match(body) against('needle' in natural language mode) order by seq;
-- @regex("(?s)Sort Key:.*Sort Key:",true)
explain select id, order_tick(id) as seq from ft2 where match(body) against('needle' in natural language mode) order by seq;
-- @regex("Fulltext Index Scan",true)
-- @regex("(?s)Sort Key:.*Sort Key:",true)
explain select id, order_tick(id) as seq from covered where match(body) against('needle' in natural language mode) order by seq;
drop function order_tick(bigint);

drop database mo30164_validation;
set ft_relevancy_algorithm = @hybrid_old_ft;
set ft2_relevancy_algorithm = @hybrid_old_ft2;
set experimental_fulltext_index = @hybrid_old_exp_ft;
set experimental_fulltext2_index = @hybrid_old_exp_ft2;
set max_dop = @hybrid_old_dop;
