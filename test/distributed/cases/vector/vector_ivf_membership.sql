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

-- ============================================================================
-- section 4: IN-subquery SEMI JOIN membership is applied before candidate limit
-- The globally nearest rows are all ineligible; the eligible nearest row ranks
-- later globally and must still be returned.
-- ============================================================================
create table ivf_semi_chunks (
    id int primary key,
    category varchar(64) not null,
    document_id varchar(64) not null,
    embedding vecf32(2)
);
insert into ivf_semi_chunks values
(1, 'cat1', 'ineligible', '[0.0,0.0]'),
(2, 'cat1', 'ineligible', '[0.1,0.1]'),
(3, 'cat1', 'ineligible', '[0.2,0.2]'),
(4, 'cat1', 'eligible',   '[1.0,1.0]'),
(5, 'cat1', 'eligible',   '[2.0,2.0]');
create table ivf_semi_filters (document_id varchar(64));
insert into ivf_semi_filters values ('eligible'), ('eligible'), (null);
create index idx_semi_category on ivf_semi_chunks(category);
create index idx_semi using ivfflat on ivf_semi_chunks(embedding) lists=1 op_type 'vector_l2_ops';

-- @regex("Vector Index Scan", true)
-- @regex("Index Table Scan.*idx_semi_category", true)
explain select c.id from ivf_semi_chunks c
where c.category = 'cat1'
  and c.document_id in (select f.document_id from ivf_semi_filters f)
  and l2_distance(c.embedding, '[0.0,0.0]') <= 10
order by l2_distance(c.embedding, '[0.0,0.0]')
limit 1 by rank with option 'mode=pre';

select c.id from ivf_semi_chunks c
where c.category = 'cat1'
  and c.document_id in (select f.document_id from ivf_semi_filters f)
  and l2_distance(c.embedding, '[0.0,0.0]') <= 10
order by l2_distance(c.embedding, '[0.0,0.0]')
limit 1 by rank with option 'mode=pre';

-- #29160: small membership domains must survive persisted entries.
set experimental_fulltext_index = 1;
create table persisted_match(id bigint primary key, body text, category varchar(8), v vecf32(3));
insert into persisted_match values (1,'alpha matrix one','A','[0,0,0]'),(2,'alpha vector search','A','[0.1,0,0]'),(3,'beta storage engine','B','[0.2,0,0]'),(4,'alpha transaction','B','[0.3,0,0]'),(5,'alpha analytics','C','[0.4,0,0]');
create fulltext index body_idx on persisted_match(body) with parser ngram;
create index vec_idx using ivfflat on persisted_match(v) lists=2 op_type 'vector_l2_ops';
-- @wait_expect(1, 30)
select id from persisted_match where match(body) against('+alpha' in boolean mode) order by id;
select id from persisted_match where match(body) against('+alpha' in boolean mode) order by l2_distance(v,'[0.04,0,0]') limit 3 by rank with option 'mode=pre';
select id from persisted_match where match(body) against('+alpha' in boolean mode) order by l2_distance(v,'[0.04,0,0]') limit 3 by rank with option 'mode=post';
select id from persisted_match where match(body) against('+alpha' in boolean mode) order by l2_distance(v,'[0.04,0,0]') limit 3 by rank with option 'mode=force';
select id from persisted_match where category='A' order by l2_distance(v,'[0.04,0,0]') limit 3 by rank with option 'mode=pre';
select id from persisted_match where category='A' order by l2_distance(v,'[0.04,0,0]') limit 3 by rank with option 'mode=post';
select id from persisted_match where category='A' order by l2_distance(v,'[0.04,0,0]') limit 3 by rank with option 'mode=force';
set @entries = (select distinct i.index_table_name from mo_catalog.mo_indexes i join mo_catalog.mo_tables t on i.table_id=t.rel_id where t.reldatabase=database() and t.relname='persisted_match' and i.name='vec_idx' and i.algo_table_type='entries');
-- @ignore:0
select mo_ctl('dn','flush',concat(database(),'.',@entries));
set @stats = concat('select table_cnt, accurate_object_number > 0 as persisted from table_stats("',database(),'.',@entries,'","refresh","full") g');
prepare entry_stats from @stats;
execute entry_stats;
deallocate prepare entry_stats;
select id from persisted_match where match(body) against('+alpha' in boolean mode) order by l2_distance(v,'[0.04,0,0]') limit 3 by rank with option 'mode=pre';
select id from persisted_match where match(body) against('+alpha' in boolean mode) order by l2_distance(v,'[0.04,0,0]') limit 3 by rank with option 'mode=post';
select id from persisted_match where match(body) against('+alpha' in boolean mode) order by l2_distance(v,'[0.04,0,0]') limit 3 by rank with option 'mode=force';
select id from persisted_match where category='A' order by l2_distance(v,'[0.04,0,0]') limit 3 by rank with option 'mode=pre';
select id from persisted_match where category='A' order by l2_distance(v,'[0.04,0,0]') limit 3 by rank with option 'mode=post';
select id from persisted_match where category='A' order by l2_distance(v,'[0.04,0,0]') limit 3 by rank with option 'mode=force';
create table persisted_int(id int primary key, grp int, v vecf32(2));
insert into persisted_int select result,result,cast(concat('[',result/1000,',0]') as vecf32(2)) from generate_series(1,200) g;
create index vec_idx using ivfflat on persisted_int(v) lists=1 op_type 'vector_l2_ops';
select id from persisted_int where grp<=0 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';
select id from persisted_int where grp<=0 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=force';
select id from persisted_int where id<=0 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';
select id from persisted_int where id<=0 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=force';
select id from persisted_int where grp<=1 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';
select id from persisted_int where grp<=1 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=force';
select id from persisted_int where id<=1 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';
select id from persisted_int where id<=1 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=force';
select id from persisted_int where grp<=99 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';
select id from persisted_int where grp<=99 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=force';
select id from persisted_int where id<=99 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';
select id from persisted_int where id<=99 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=force';
select id from persisted_int where grp<=100 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';
select id from persisted_int where grp<=100 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=force';
select id from persisted_int where id<=100 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';
select id from persisted_int where id<=100 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=force';
select id from persisted_int where grp<=101 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';
select id from persisted_int where grp<=101 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=force';
select id from persisted_int where id<=101 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';
select id from persisted_int where id<=101 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=force';
set @entries = (select distinct i.index_table_name from mo_catalog.mo_indexes i join mo_catalog.mo_tables t on i.table_id=t.rel_id where t.reldatabase=database() and t.relname='persisted_int' and i.name='vec_idx' and i.algo_table_type='entries');
-- @ignore:0
select mo_ctl('dn','flush',concat(database(),'.',@entries));
set @stats = concat('select table_cnt, accurate_object_number > 0 as persisted from table_stats("',database(),'.',@entries,'","refresh","full") g');
prepare entry_stats from @stats;
execute entry_stats;
deallocate prepare entry_stats;
select id from persisted_int where grp<=0 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';
select id from persisted_int where grp<=0 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=force';
select id from persisted_int where id<=0 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';
select id from persisted_int where id<=0 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=force';
select id from persisted_int where grp<=1 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';
select id from persisted_int where grp<=1 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=force';
select id from persisted_int where id<=1 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';
select id from persisted_int where id<=1 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=force';
select id from persisted_int where grp<=99 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';
select id from persisted_int where grp<=99 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=force';
select id from persisted_int where id<=99 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';
select id from persisted_int where id<=99 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=force';
select id from persisted_int where grp<=100 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';
select id from persisted_int where grp<=100 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=force';
select id from persisted_int where id<=100 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';
select id from persisted_int where id<=100 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=force';
select id from persisted_int where grp<=101 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';
select id from persisted_int where grp<=101 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=force';
select id from persisted_int where id<=101 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';
select id from persisted_int where id<=101 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=force';
create table persisted_bigint(id bigint primary key, grp int, v vecf32(2));
insert into persisted_bigint select result,result,cast(concat('[',result/1000,',0]') as vecf32(2)) from generate_series(1,200) g;
create index vec_idx using ivfflat on persisted_bigint(v) lists=1 op_type 'vector_l2_ops';
select id from persisted_bigint where grp<=0 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';
select id from persisted_bigint where grp<=0 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=force';
select id from persisted_bigint where id<=0 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';
select id from persisted_bigint where id<=0 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=force';
select id from persisted_bigint where grp<=1 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';
select id from persisted_bigint where grp<=1 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=force';
select id from persisted_bigint where id<=1 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';
select id from persisted_bigint where id<=1 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=force';
select id from persisted_bigint where grp<=99 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';
select id from persisted_bigint where grp<=99 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=force';
select id from persisted_bigint where id<=99 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';
select id from persisted_bigint where id<=99 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=force';
select id from persisted_bigint where grp<=100 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';
select id from persisted_bigint where grp<=100 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=force';
select id from persisted_bigint where id<=100 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';
select id from persisted_bigint where id<=100 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=force';
select id from persisted_bigint where grp<=101 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';
select id from persisted_bigint where grp<=101 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=force';
select id from persisted_bigint where id<=101 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';
select id from persisted_bigint where id<=101 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=force';
set @entries = (select distinct i.index_table_name from mo_catalog.mo_indexes i join mo_catalog.mo_tables t on i.table_id=t.rel_id where t.reldatabase=database() and t.relname='persisted_bigint' and i.name='vec_idx' and i.algo_table_type='entries');
-- @ignore:0
select mo_ctl('dn','flush',concat(database(),'.',@entries));
set @stats = concat('select table_cnt, accurate_object_number > 0 as persisted from table_stats("',database(),'.',@entries,'","refresh","full") g');
prepare entry_stats from @stats;
execute entry_stats;
deallocate prepare entry_stats;
select id from persisted_bigint where grp<=0 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';
select id from persisted_bigint where grp<=0 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=force';
select id from persisted_bigint where id<=0 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';
select id from persisted_bigint where id<=0 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=force';
select id from persisted_bigint where grp<=1 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';
select id from persisted_bigint where grp<=1 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=force';
select id from persisted_bigint where id<=1 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';
select id from persisted_bigint where id<=1 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=force';
select id from persisted_bigint where grp<=99 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';
select id from persisted_bigint where grp<=99 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=force';
select id from persisted_bigint where id<=99 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';
select id from persisted_bigint where id<=99 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=force';
select id from persisted_bigint where grp<=100 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';
select id from persisted_bigint where grp<=100 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=force';
select id from persisted_bigint where id<=100 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';
select id from persisted_bigint where id<=100 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=force';
select id from persisted_bigint where grp<=101 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';
select id from persisted_bigint where grp<=101 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=force';
select id from persisted_bigint where id<=101 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=pre';
select id from persisted_bigint where id<=101 order by l2_distance(v,'[0,0]') limit 10 by rank with option 'mode=force';

set ivf_preload_entries = 0;
set probe_limit = 5;
drop database ivf_membership;
