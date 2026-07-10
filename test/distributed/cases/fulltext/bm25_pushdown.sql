-- bm25 filtered retrieval with fulltext_bloom_filter_pushdown=ON. A MATCH combined
-- with an extra non-MATCH WHERE filter builds the pre-filter 2-JOIN that pushes the
-- predicate into the WAND walk as a membership bitset (so bm25_search only scores the
-- qualifying docs). The pushdown is a pure performance optimization: results MUST be
-- identical to the pushdown=OFF path. This exercises the 2-JOIN membership-bitset
-- prefilter, which the other bm25 cases (no extra WHERE) never reach.
drop database if exists bm25_pushdown;
create database bm25_pushdown;
use bm25_pushdown;
create table docs (id bigint primary key, body text, cat int);
insert into docs values
(1,'apple banana cherry',10),
(2,'apple banana',20),
(3,'apple',10),
(4,'durian mango',10),
(5,'apple apple apple banana',20),
(6,'apple cherry',10),
(7,'banana cherry',20);
create index ftx using bm25 on docs(body) with parser gojieba;

-- ===== baseline: pushdown OFF (single JOIN, filter applied after search) =====
set fulltext_bloom_filter_pushdown=off;
-- apple ∩ cat=10 -> {1,3,6}
select id from docs where match(body) against('apple') and cat=10 order by id;
-- apple ∩ cat=20 -> {2,5}
select id from docs where match(body) against('apple') and cat=20 order by id;
-- ranked top-2 of apple ∩ cat=10
select id from docs where match(body) against('apple') and cat=10 limit 2;
-- two-term MATCH + filter
select id from docs where match(body) against('apple banana') and cat=20 order by id;
-- filter selects rows the term does not match -> empty
select id from docs where match(body) against('durian') and cat=20 order by id;
-- filter matches nothing -> empty
select id from docs where match(body) against('apple') and cat=99 order by id;
-- count with MATCH + filter
select count(*) from docs where match(body) against('apple') and cat=10;

-- ===== pushdown ON (2-JOIN membership-bitset prefilter): SAME rows =====
set fulltext_bloom_filter_pushdown=on;
select id from docs where match(body) against('apple') and cat=10 order by id;
select id from docs where match(body) against('apple') and cat=20 order by id;
select id from docs where match(body) against('apple') and cat=10 limit 2;
select id from docs where match(body) against('apple banana') and cat=20 order by id;
select id from docs where match(body) against('durian') and cat=20 order by id;
select id from docs where match(body) against('apple') and cat=99 order by id;
select count(*) from docs where match(body) against('apple') and cat=10;

set fulltext_bloom_filter_pushdown=off;
drop database bm25_pushdown;
