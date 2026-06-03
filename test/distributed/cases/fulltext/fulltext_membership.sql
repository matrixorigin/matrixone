-- BVT: doc_id membership-filter pushdown for the fulltext index, across all
-- three filter structures that docfilter.Build selects based on the source
-- table's primary-key type:
--   section 1: integer PK, small/bounded ids        -> dense cbitmap
--   section 2: integer PK, wide id span (> 2^23)     -> compact CRoaring bitset
--   section 3: varchar (non-integer) PK              -> CBloomFilter (approx)
-- The pushdown is transparent to query results, so each section verifies that
-- "match(...) against(...) AND <relational predicate>" returns the correct rows
-- (the predicate produces the candidate doc_id set that builds the filter).

set fulltext_bloom_filter_pushdown = 1;

drop database if exists ft_membership;
create database ft_membership;
use ft_membership;

-- ============================================================================
-- section 1: integer PK, small ids -> cbitmap
-- ============================================================================
create table ft_cbitmap (
    id bigint primary key,
    title varchar(200) not null,
    body text not null,
    category varchar(50) not null
);
insert into ft_cbitmap values
(1,  'Introduction to Machine Learning', 'machine learning is a subset of artificial intelligence', 'tech'),
(2,  'Deep Learning Fundamentals',       'deep learning uses neural networks to learn from data', 'tech'),
(3,  'Cooking with Machine Learning',    'using machine learning to recommend recipes from ingredients', 'food'),
(4,  'The Art of French Cooking',        'french cooking techniques from around the world', 'food'),
(5,  'Sports Analytics with ML',         'machine learning is transforming sports analytics today', 'sports'),
(6,  'Running for Beginners',            'a beginners guide to running and training plans', 'sports'),
(7,  'Machine Learning in Finance',      'machine learning for fraud detection and risk control', 'finance'),
(8,  'Investment Strategies',            'long term investment strategies for building wealth', 'finance'),
(9,  'Machine Learning Research',        'a curated list of machine learning research papers', 'tech'),
(10, 'Data Engineering Practices',       'best practices for data pipelines and infrastructure', 'tech'),
(11, 'Reinforcement Learning',           'reinforcement learning in robotics and gaming systems', 'tech'),
(12, 'Transfer Learning Guide',          'transfer learning reuses pretrained machine learning models', 'tech');

create fulltext index ftidx_cbitmap on ft_cbitmap (title, body);

select id, title, category from ft_cbitmap
where match(title, body) against('machine learning') and category = 'tech'
order by id;

select id, category from ft_cbitmap
where match(title, body) against('machine learning') and id > 6
order by id;

select count(*) from ft_cbitmap
where match(title, body) against('machine learning') and category = 'tech';

-- ============================================================================
-- section 2: integer PK, wide id span (> 2^23 = 8388608) -> CRoaring
-- The candidate doc_id span (tech rows: 2000000 .. 24000000 = 22M) exceeds
-- MaxCbitmapBits even with the base offset (on by default), so the dense cbitmap
-- is infeasible and docfilter.Build falls back to the compact CRoaring bitset.
-- ============================================================================
create table ft_croaring (
    id bigint primary key,
    title varchar(200) not null,
    body text not null,
    category varchar(50) not null
);
insert into ft_croaring values
(2000000,   'Introduction to Machine Learning', 'machine learning is a subset of artificial intelligence', 'tech'),
(4000000,   'Deep Learning Fundamentals',       'deep learning uses neural networks to learn from data', 'tech'),
(6000000,   'Cooking with Machine Learning',    'using machine learning to recommend recipes from ingredients', 'food'),
(8000000,   'The Art of French Cooking',        'french cooking techniques from around the world', 'food'),
(10000000,  'Sports Analytics with ML',         'machine learning is transforming sports analytics today', 'sports'),
(12000000,  'Running for Beginners',            'a beginners guide to running and training plans', 'sports'),
(14000000,  'Machine Learning in Finance',      'machine learning for fraud detection and risk control', 'finance'),
(16000000,  'Investment Strategies',            'long term investment strategies for building wealth', 'finance'),
(18000000,  'Machine Learning Research',        'a curated list of machine learning research papers', 'tech'),
(20000000,  'Data Engineering Practices',       'best practices for data pipelines and infrastructure', 'tech'),
(22000000,  'Reinforcement Learning',           'reinforcement learning in robotics and gaming systems', 'tech'),
(24000000,  'Transfer Learning Guide',          'transfer learning reuses pretrained machine learning models', 'tech');

create fulltext index ftidx_croaring on ft_croaring (title, body);

select id, title, category from ft_croaring
where match(title, body) against('machine learning') and category = 'tech'
order by id;

select count(*) from ft_croaring
where match(title, body) against('machine learning') and category = 'tech';

-- ============================================================================
-- section 3: varchar PK -> CBloomFilter
-- ============================================================================
create table ft_bloom (
    id varchar(64) primary key,
    title varchar(200) not null,
    body text not null,
    category varchar(50) not null
);
insert into ft_bloom values
('doc-01', 'Introduction to Machine Learning', 'machine learning is a subset of artificial intelligence', 'tech'),
('doc-02', 'Deep Learning Fundamentals',       'deep learning uses neural networks to learn from data', 'tech'),
('doc-03', 'Cooking with Machine Learning',    'using machine learning to recommend recipes from ingredients', 'food'),
('doc-04', 'The Art of French Cooking',        'french cooking techniques from around the world', 'food'),
('doc-05', 'Sports Analytics with ML',         'machine learning is transforming sports analytics today', 'sports'),
('doc-06', 'Running for Beginners',            'a beginners guide to running and training plans', 'sports'),
('doc-07', 'Machine Learning in Finance',      'machine learning for fraud detection and risk control', 'finance'),
('doc-08', 'Investment Strategies',            'long term investment strategies for building wealth', 'finance'),
('doc-09', 'Machine Learning Research',        'a curated list of machine learning research papers', 'tech'),
('doc-10', 'Data Engineering Practices',       'best practices for data pipelines and infrastructure', 'tech'),
('doc-11', 'Reinforcement Learning',           'reinforcement learning in robotics and gaming systems', 'tech'),
('doc-12', 'Transfer Learning Guide',          'transfer learning reuses pretrained machine learning models', 'tech');

create fulltext index ftidx_bloom on ft_bloom (title, body);

select id, title, category from ft_bloom
where match(title, body) against('machine learning') and category = 'tech'
order by id;

select count(*) from ft_bloom
where match(title, body) against('machine learning') and category = 'tech';

set fulltext_bloom_filter_pushdown = 0;
drop database ft_membership;
