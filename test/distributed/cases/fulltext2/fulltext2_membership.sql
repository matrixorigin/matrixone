-- fulltext2 membership-filter pushdown (the WHERE-clause prefilter ported from
-- classic fulltext's fulltext_membership + bm25_pushdown). A MATCH combined with a
-- non-MATCH WHERE predicate builds the pre-filter 2-JOIN that pushes the predicate
-- into the WAND walk as a membership bitset, so fulltext2_search only scores the
-- qualifying docs. The pushdown is a pure performance optimization: results MUST be
-- identical to the pushdown=OFF path. This also exercises all three filter
-- structures docfilter.Build selects by primary-key type:
--   section 1: integer PK, small ids            -> dense cbitmap (+ OFF==ON parity)
--   section 2: integer PK, wide id span (>2^23) -> compact CRoaring bitset
--   section 3: varchar (non-integer) PK         -> CBloomFilter (approx)
set experimental_fulltext2_index = 1;
drop database if exists fulltext2_pushdown;
create database fulltext2_pushdown;
use fulltext2_pushdown;

-- ============================================================================
-- section 1: integer PK, small ids -> cbitmap. Run every query pushdown OFF then
-- ON: the two blocks MUST return identical rows (the prefilter is transparent).
-- ============================================================================
create table ft_cbitmap (id bigint primary key, body text not null, category varchar(50) not null);
insert into ft_cbitmap values
(1,  'machine learning is a subset of artificial intelligence', 'tech'),
(2,  'deep learning uses neural networks to learn from data', 'tech'),
(3,  'using machine learning to recommend recipes from ingredients', 'food'),
(4,  'french cooking techniques from around the world', 'food'),
(5,  'machine learning is transforming sports analytics today', 'sports'),
(6,  'a beginners guide to running and training plans', 'sports'),
(7,  'machine learning for fraud detection and risk control', 'finance'),
(8,  'long term investment strategies for building wealth', 'finance'),
(9,  'a curated list of machine learning research papers', 'tech'),
(10, 'best practices for data pipelines and infrastructure', 'tech'),
(11, 'reinforcement learning in robotics and gaming systems', 'tech'),
(12, 'transfer learning reuses pretrained machine learning models', 'tech');
create fulltext2 index ftidx_cbitmap on ft_cbitmap (body);

-- ---- pushdown OFF (baseline: filter applied after search) ----
set fulltext_bloom_filter_pushdown = 0;
select id, category from ft_cbitmap where match(body) against('learning') and category = 'tech' order by id;
select id, category from ft_cbitmap where match(body) against('learning') and id > 6 order by id;
select id from ft_cbitmap where match(body) against('learning') and category = 'food' order by id;
select id from ft_cbitmap where match(body) against('investment') and category = 'tech' order by id;
select id from ft_cbitmap where match(body) against('learning') and category = 'nope' order by id;
select count(*) from ft_cbitmap where match(body) against('learning') and category = 'tech';

-- ---- pushdown ON (2-JOIN membership-bitset prefilter): SAME rows ----
set fulltext_bloom_filter_pushdown = 1;
select id, category from ft_cbitmap where match(body) against('learning') and category = 'tech' order by id;
select id, category from ft_cbitmap where match(body) against('learning') and id > 6 order by id;
select id from ft_cbitmap where match(body) against('learning') and category = 'food' order by id;
select id from ft_cbitmap where match(body) against('investment') and category = 'tech' order by id;
select id from ft_cbitmap where match(body) against('learning') and category = 'nope' order by id;
select count(*) from ft_cbitmap where match(body) against('learning') and category = 'tech';

-- ============================================================================
-- section 2: integer PK, wide id span (>2^23) -> CRoaring bitset
-- ============================================================================
create table ft_croaring (id bigint primary key, body text not null, category varchar(50) not null);
insert into ft_croaring values
(2000000,   'machine learning is a subset of artificial intelligence', 'tech'),
(4000000,   'deep learning uses neural networks to learn from data', 'tech'),
(6000000,   'using machine learning to recommend recipes from ingredients', 'food'),
(8000000,   'french cooking techniques from around the world', 'food'),
(10000000,  'machine learning is transforming sports analytics today', 'sports'),
(12000000,  'a beginners guide to running and training plans', 'sports'),
(14000000,  'machine learning for fraud detection and risk control', 'finance'),
(16000000,  'long term investment strategies for building wealth', 'finance'),
(18000000,  'a curated list of machine learning research papers', 'tech'),
(20000000,  'best practices for data pipelines and infrastructure', 'tech'),
(22000000,  'reinforcement learning in robotics and gaming systems', 'tech'),
(24000000,  'transfer learning reuses pretrained machine learning models', 'tech');
create fulltext2 index ftidx_croaring on ft_croaring (body);
select id, category from ft_croaring where match(body) against('learning') and category = 'tech' order by id;
select count(*) from ft_croaring where match(body) against('learning') and category = 'tech';

-- ============================================================================
-- section 3: varchar PK -> CBloomFilter (approximate; join to source removes any
-- false positive, so the returned rows are still exact)
-- ============================================================================
create table ft_bloom (id varchar(64) primary key, body text not null, category varchar(50) not null);
insert into ft_bloom values
('doc-01', 'machine learning is a subset of artificial intelligence', 'tech'),
('doc-02', 'deep learning uses neural networks to learn from data', 'tech'),
('doc-03', 'using machine learning to recommend recipes from ingredients', 'food'),
('doc-04', 'french cooking techniques from around the world', 'food'),
('doc-05', 'machine learning is transforming sports analytics today', 'sports'),
('doc-06', 'a beginners guide to running and training plans', 'sports'),
('doc-07', 'machine learning for fraud detection and risk control', 'finance'),
('doc-08', 'long term investment strategies for building wealth', 'finance'),
('doc-09', 'a curated list of machine learning research papers', 'tech'),
('doc-10', 'best practices for data pipelines and infrastructure', 'tech'),
('doc-11', 'reinforcement learning in robotics and gaming systems', 'tech'),
('doc-12', 'transfer learning reuses pretrained machine learning models', 'tech');
create fulltext2 index ftidx_bloom on ft_bloom (body);
select id, category from ft_bloom where match(body) against('learning') and category = 'tech' order by id;
select count(*) from ft_bloom where match(body) against('learning') and category = 'tech';

set fulltext_bloom_filter_pushdown = 0;
drop database fulltext2_pushdown;
