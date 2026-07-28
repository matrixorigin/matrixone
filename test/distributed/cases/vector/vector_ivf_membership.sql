-- BVT: doc_id membership-filter pre-pushdown for the ivfflat index, across all
-- three filter structures that docfilter.Build selects based on the source
-- table's primary-key type:
--   section 1: integer PK, small/bounded ids        -> dense cbitmap
--   section 2: integer PK, wide id span (> 2^23)     -> compact CRoaring bitset
--   section 3: varchar (non-integer) PK              -> CBloomFilter (approx)
-- In "mode=pre" the relational predicate (category/score) builds the candidate
-- PK filter that prunes the vector search. The filter is transparent to
-- results, so each section verifies the same ranked output. lists=2 with
-- probe_limit>=lists probes every centroid, so results are deterministic.

set ivf_preload_entries = 0;
set probe_limit = 5;

drop database if exists ivf_membership;
create database ivf_membership;
use ivf_membership;

-- ============================================================================
-- section 1: integer PK, small ids -> cbitmap
-- ============================================================================
create table ivf_cbitmap (
    id int primary key,
    category varchar(50),
    score float,
    embedding vecf32(8)
);
insert into ivf_cbitmap values
(1,  'cat1', 5.0, '[0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1]'),
(2,  'cat1', 4.5, '[0.2,0.2,0.2,0.2,0.2,0.2,0.2,0.2]'),
(3,  'cat2', 4.0, '[0.3,0.3,0.3,0.3,0.3,0.3,0.3,0.3]'),
(4,  'cat2', 3.5, '[0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4]'),
(5,  'cat3', 3.0, '[0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5]'),
(6,  'cat3', 2.5, '[0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6]'),
(7,  'cat1', 2.0, '[0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7]'),
(8,  'cat2', 1.5, '[0.8,0.8,0.8,0.8,0.8,0.8,0.8,0.8]'),
(9,  'cat3', 1.0, '[0.9,0.9,0.9,0.9,0.9,0.9,0.9,0.9]'),
(10, 'cat1', 0.5, '[1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0]');

create index idx_cbitmap using ivfflat on ivf_cbitmap(embedding) lists=2 op_type 'vector_l2_ops';

select id, category, score from ivf_cbitmap
where category = 'cat1' and score > 2.0
order by l2_distance(embedding, '[0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1]')
limit 3 by rank with option 'mode=pre';

-- ============================================================================
-- section 2: integer PK, wide id span (> 2^23 = 8388608) -> CRoaring
-- The candidate PK span (cat1 & score>2.0 rows: 10000000 .. 20000000 = 10M)
-- exceeds MaxCbitmapBits even with the base offset (on by default), so the dense
-- cbitmap is infeasible and docfilter.Build falls back to the CRoaring bitset.
-- ============================================================================
create table ivf_croaring (
    id bigint primary key,
    category varchar(50),
    score float,
    embedding vecf32(8)
);
insert into ivf_croaring values
(10000000,  'cat1', 5.0, '[0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1]'),
(20000000,  'cat1', 4.5, '[0.2,0.2,0.2,0.2,0.2,0.2,0.2,0.2]'),
(30000000,  'cat2', 4.0, '[0.3,0.3,0.3,0.3,0.3,0.3,0.3,0.3]'),
(40000000,  'cat2', 3.5, '[0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4]'),
(50000000,  'cat3', 3.0, '[0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5]'),
(60000000,  'cat3', 2.5, '[0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6]'),
(70000000,  'cat1', 2.0, '[0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7]'),
(80000000,  'cat2', 1.5, '[0.8,0.8,0.8,0.8,0.8,0.8,0.8,0.8]'),
(90000000,  'cat3', 1.0, '[0.9,0.9,0.9,0.9,0.9,0.9,0.9,0.9]'),
(100000000, 'cat1', 0.5, '[1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0]');

create index idx_croaring using ivfflat on ivf_croaring(embedding) lists=2 op_type 'vector_l2_ops';

select id, category, score from ivf_croaring
where category = 'cat1' and score > 2.0
order by l2_distance(embedding, '[0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1]')
limit 3 by rank with option 'mode=pre';

-- ============================================================================
-- section 3: varchar PK -> CBloomFilter
-- ============================================================================
create table ivf_bloom (
    id varchar(64) primary key,
    category varchar(50),
    score float,
    embedding vecf32(8)
);
insert into ivf_bloom values
('v01', 'cat1', 5.0, '[0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1]'),
('v02', 'cat1', 4.5, '[0.2,0.2,0.2,0.2,0.2,0.2,0.2,0.2]'),
('v03', 'cat2', 4.0, '[0.3,0.3,0.3,0.3,0.3,0.3,0.3,0.3]'),
('v04', 'cat2', 3.5, '[0.4,0.4,0.4,0.4,0.4,0.4,0.4,0.4]'),
('v05', 'cat3', 3.0, '[0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5]'),
('v06', 'cat3', 2.5, '[0.6,0.6,0.6,0.6,0.6,0.6,0.6,0.6]'),
('v07', 'cat1', 2.0, '[0.7,0.7,0.7,0.7,0.7,0.7,0.7,0.7]'),
('v08', 'cat2', 1.5, '[0.8,0.8,0.8,0.8,0.8,0.8,0.8,0.8]'),
('v09', 'cat3', 1.0, '[0.9,0.9,0.9,0.9,0.9,0.9,0.9,0.9]'),
('v10', 'cat1', 0.5, '[1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0]');

create index idx_bloom using ivfflat on ivf_bloom(embedding) lists=2 op_type 'vector_l2_ops';

select id, category, score from ivf_bloom
where category = 'cat1' and score > 2.0
order by l2_distance(embedding, '[0.1,0.1,0.1,0.1,0.1,0.1,0.1,0.1]')
limit 3 by rank with option 'mode=pre';

set ivf_preload_entries = 0;
set probe_limit = 5;
drop database ivf_membership;
