-- BVT: doc_id membership-filter pushdown for the fulltext index, across all
-- three filter structures that docfilter.Build selects based on the source
-- table's primary-key type:
--   section 1: integer PK, small/bounded ids        -> dense cbitmap
--   section 2: integer PK, large ids (> 2^27)        -> compact CRoaring bitset
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
-- section 2: integer PK, large ids (> 2^27 = 134217728) -> CRoaring
-- ============================================================================
create table ft_croaring (
    id bigint primary key,
    title varchar(200) not null,
    body text not null,
    category varchar(50) not null
);
insert into ft_croaring values
(200000001, 'Introduction to Machine Learning', 'machine learning is a subset of artificial intelligence', 'tech'),
(200000002, 'Deep Learning Fundamentals',       'deep learning uses neural networks to learn from data', 'tech'),
(200000003, 'Cooking with Machine Learning',    'using machine learning to recommend recipes from ingredients', 'food'),
(200000004, 'The Art of French Cooking',        'french cooking techniques from around the world', 'food'),
(200000005, 'Sports Analytics with ML',         'machine learning is transforming sports analytics today', 'sports'),
(200000006, 'Running for Beginners',            'a beginners guide to running and training plans', 'sports'),
(200000007, 'Machine Learning in Finance',      'machine learning for fraud detection and risk control', 'finance'),
(200000008, 'Investment Strategies',            'long term investment strategies for building wealth', 'finance'),
(200000009, 'Machine Learning Research',        'a curated list of machine learning research papers', 'tech'),
(200000010, 'Data Engineering Practices',       'best practices for data pipelines and infrastructure', 'tech'),
(200000011, 'Reinforcement Learning',           'reinforcement learning in robotics and gaming systems', 'tech'),
(200000012, 'Transfer Learning Guide',          'transfer learning reuses pretrained machine learning models', 'tech');

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
