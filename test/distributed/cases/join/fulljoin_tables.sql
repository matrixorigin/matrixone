-- =============================================================
-- FULL OUTER JOIN coverage of MO's table kinds and indexes
--
-- Table kinds exercised:
--   - normal table
--   - temporary table
--   - partitioned table (HASH / RANGE / KEY / LIST)
--   - view
--   - CTE / recursive CTE / derived table
--   - cluster table  (mo_catalog only; sys tenant required)
--   - table with fulltext index
--   - table with vector index (IVFFLAT / HNSW)
--
-- NOTE: As of the current MO release, MATCH() AGAINST() cannot
--   coexist with FULL OUTER JOIN in the same query (planner limit:
--   "MATCH AGAINST cannot be replaced by FULLTEXT INDEX and full
--   table scan with fulltext search is not supported yet"). So
--   fulltext cases here join the fulltext-indexed table as a
--   normal (non-MATCH) participant, plus a separate MATCH query
--   is run against it to prove the index still works standalone.
--   Update this file when the planner limitation is lifted.
-- =============================================================

drop database if exists fulljoin_tbl_db;
create database fulljoin_tbl_db;
use fulljoin_tbl_db;

-- -------------------------------------------------------------
-- 1. Temporary table FULL OUTER JOIN normal table
-- -------------------------------------------------------------
drop table if exists normal_t;
create table normal_t(id int primary key, v varchar(10));
insert into normal_t values (1,'n1'),(2,'n2'),(3,'n3');

drop table if exists tmp_t;
create temporary table tmp_t(id int primary key, v varchar(10));
insert into tmp_t values (2,'t2'),(3,'t3'),(4,'t4');

select 'tmp_full_normal' as tag;
select * from tmp_t full outer join normal_t on tmp_t.id = normal_t.id order by tmp_t.id, normal_t.id;

select 'normal_full_tmp' as tag;
select * from normal_t full outer join tmp_t on tmp_t.id = normal_t.id order by tmp_t.id, normal_t.id;

-- Temp table on both sides
drop table if exists tmp_l;
drop table if exists tmp_r;
create temporary table tmp_l(id int, v varchar(5));
create temporary table tmp_r(id int, v varchar(5));
insert into tmp_l values (1,'L1'),(2,'L2'),(NULL,'Ln');
insert into tmp_r values (2,'R2'),(3,'R3'),(NULL,'Rn');
select 'tmp_full_tmp' as tag;
select * from tmp_l full outer join tmp_r on tmp_l.id = tmp_r.id order by tmp_l.id, tmp_r.id;

drop table if exists tmp_t;
drop table if exists tmp_l;
drop table if exists tmp_r;

-- -------------------------------------------------------------
-- 2. Partitioned tables
-- -------------------------------------------------------------

-- 2.1 HASH partition
drop table if exists ph;
create table ph(a int, v varchar(5)) partition by hash(a) partitions 4;
insert into ph values (1,'a'),(2,'b'),(5,'c'),(7,'d'),(NULL,'n');

-- 2.2 RANGE partition
drop table if exists pr;
create table pr(a int, v varchar(5)) partition by range(a)(
    partition p0 values less than (10),
    partition p1 values less than (20),
    partition p2 values less than (30)
);
insert into pr values (1,'a'),(5,'b'),(15,'c'),(25,'d');

-- 2.3 KEY partition
drop table if exists pk;
create table pk(a int, v varchar(5)) partition by key(a) partitions 3;
insert into pk values (1,'a'),(2,'b'),(3,'c'),(4,'d');

-- 2.4 LIST partition
drop table if exists pl;
create table pl(a int, v varchar(5)) partition by list(a)(
    partition l0 values in (1,2,3),
    partition l1 values in (10,20,30)
);
insert into pl values (1,'a'),(10,'b'),(20,'c'),(2,'d');

drop table if exists ref;
create table ref(a int, label varchar(5));
insert into ref values (1,'R1'),(3,'R3'),(15,'R15'),(20,'R20'),(99,'R99');

select 'part_hash'  as tag; select * from ph full outer join ref on ph.a = ref.a order by ph.a, ref.a;
select 'part_range' as tag; select * from pr full outer join ref on pr.a = ref.a order by pr.a, ref.a;
select 'part_key'   as tag; select * from pk full outer join ref on pk.a = ref.a order by pk.a, ref.a;
select 'part_list'  as tag; select * from pl full outer join ref on pl.a = ref.a order by pl.a, ref.a;

-- 2.5 FULL JOIN between two partitioned tables
select 'part_full_part' as tag;
select * from ph full outer join pr on ph.a = pr.a order by ph.a, pr.a;

-- -------------------------------------------------------------
-- 3. Views
-- -------------------------------------------------------------
drop view if exists v_ph;
drop view if exists v_ref;
create view v_ph  as select * from ph where a is not null;
create view v_ref as select * from ref;
select 'view_full_view' as tag;
select * from v_ph full outer join v_ref on v_ph.a = v_ref.a order by v_ph.a, v_ref.a;

-- View joined to a base table
select 'view_full_base' as tag;
select * from v_ph full outer join pr on v_ph.a = pr.a order by v_ph.a, pr.a;

drop view v_ph;
drop view v_ref;

-- -------------------------------------------------------------
-- 4. CTE / recursive CTE / derived table on one or both sides
-- -------------------------------------------------------------
drop table if exists dims;
create table dims(id int primary key, label varchar(5));
insert into dims values (1,'d1'),(2,'d2'),(4,'d4');

-- CTE on left
select 'cte_full_base' as tag;
with cte_a as (select id, label from dims where id >= 2)
select * from cte_a full outer join ref on cte_a.id = ref.a order by cte_a.id, ref.a;

-- Recursive CTE generating 1..5
select 'rec_cte_full' as tag;
with recursive nums(n) as (
    select 1 union all select n+1 from nums where n < 5
)
select nums.n left_n, ref.a right_a, ref.label
from nums full outer join ref on nums.n = ref.a
order by nums.n, ref.a;

-- Derived tables on both sides
select 'derived_full_derived' as tag;
select * from (select a, v from pr where a < 20) l
full outer join (select a, label from ref) r
on l.a = r.a
order by l.a, r.a;

-- -------------------------------------------------------------
-- 5. Cluster table (sys tenant only; cluster tables must live in
--    mo_catalog). Isolate in its own session so the other cases
--    are unaffected if the account changes.
-- -------------------------------------------------------------
-- @session:id=1&user=sys:dump&password=111
use mo_catalog;
drop table if exists fj_cluster;
create cluster table fj_cluster(a int primary key, v varchar(10));
insert into fj_cluster(a, v) values (1,'c1'),(2,'c2'),(3,'c3');

use fulljoin_tbl_db;
drop table if exists fj_peer;
create table fj_peer(a int primary key, label varchar(10));
insert into fj_peer values (1,'P1'),(3,'P3'),(5,'P5');

select 'cluster_full_normal' as tag;
select mo_catalog.fj_cluster.a c_a, mo_catalog.fj_cluster.v c_v, fj_peer.a p_a, fj_peer.label
from mo_catalog.fj_cluster full outer join fj_peer
  on mo_catalog.fj_cluster.a = fj_peer.a
order by c_a, p_a;

drop table mo_catalog.fj_cluster;
-- @session

-- -------------------------------------------------------------
-- 6. Table with FULLTEXT index
--    FULL JOIN must work on a fulltext-indexed table as long as
--    MATCH() AGAINST() is not used inside the FULL-joined query.
-- -------------------------------------------------------------
drop table if exists ft_src;
create table ft_src(
    id int primary key,
    body varchar(200),
    title varchar(100),
    fulltext key ftidx(body, title)
);
insert into ft_src values
    (1,'color is red','t1'),
    (2,'car is yellow','crazy car'),
    (3,'sky is blue','no limit'),
    (4,'blue is not red','colorful'),
    (5, NULL, 'placeholder'),
    (6, 'lone body', NULL);

drop table if exists ft_dim;
create table ft_dim(id int primary key, cat varchar(10));
insert into ft_dim values (1,'c_red'),(3,'c_blue'),(7,'c_other');

-- 6.1 Plain FULL JOIN on a fulltext-indexed table (no MATCH)
select 'ft_plain_full' as tag;
select ft_src.id, ft_src.body, ft_src.title, ft_dim.id, ft_dim.cat
from ft_src full outer join ft_dim on ft_src.id = ft_dim.id
order by ft_src.id, ft_dim.id;

-- 6.2 SHOW INDEX confirms fulltext index is intact and the query
--     above didn't disturb it.
show index from ft_src where key_name = 'ftidx';

-- 6.3 Standalone MATCH query (verifies the index works); cannot
--     be combined with FULL OUTER JOIN at present.
select 'ft_match_standalone' as tag;
select id, body, title from ft_src where match(body, title) against('red') order by id;

-- -------------------------------------------------------------
-- 7. Table with VECTOR index (IVFFLAT)
-- -------------------------------------------------------------
set experimental_ivf_index = 1;
drop table if exists vi_src;
create table vi_src(id int primary key, emb vecf32(3));
insert into vi_src values
    (1,'[1,2,3]'),
    (2,'[1,2,4]'),
    (3,'[1,3,5]'),
    (4,'[100,44,50]'),
    (5,'[120,50,70]');
create index vi_idx using ivfflat on vi_src(emb) lists = 2 op_type 'vector_l2_ops';

drop table if exists vi_dim;
create table vi_dim(id int primary key, label varchar(10));
insert into vi_dim values (1,'V1'),(3,'V3'),(4,'V4'),(9,'V9');

-- 7.1 FULL JOIN on a table that has an IVFFLAT index (join on PK)
select 'ivf_full_plain' as tag;
select vi_src.id s_id, vi_src.emb, vi_dim.id d_id, vi_dim.label
from vi_src full outer join vi_dim on vi_src.id = vi_dim.id
order by vi_src.id, vi_dim.id;

-- 7.2 Wrap a vector-distance search in a derived table, then FULL JOIN
select 'ivf_full_knn_derived' as tag;
select knn.id k_id, knn.dist, vi_dim.id d_id, vi_dim.label
from (
    select id, l2_distance(emb, cast('[1,2,3]' as vecf32(3))) dist
    from vi_src
    order by dist
    limit 3
) knn
full outer join vi_dim on knn.id = vi_dim.id
order by knn.id, vi_dim.id;

-- -------------------------------------------------------------
-- 8. Table with VECTOR index (HNSW)
-- -------------------------------------------------------------
set experimental_hnsw_index = 1;
drop table if exists vh_src;
create table vh_src(a bigint primary key, b vecf32(3));
insert into vh_src values
    (1,'[1,2,3]'),
    (2,'[1,2,4]'),
    (3,'[1,3,5]'),
    (4,'[100,44,50]'),
    (5,'[120,50,70]');
create index vh_idx using hnsw on vh_src(b) M 4 EF_CONSTRUCTION 64 EF_SEARCH 32 OP_TYPE 'vector_l2_ops';

drop table if exists vh_dim;
create table vh_dim(a bigint primary key, label varchar(10));
insert into vh_dim values (1,'H1'),(3,'H3'),(6,'H6');

-- 8.1 FULL JOIN on a table that has an HNSW index (join on PK)
select 'hnsw_full_plain' as tag;
select vh_src.a s_a, vh_src.b, vh_dim.a d_a, vh_dim.label
from vh_src full outer join vh_dim on vh_src.a = vh_dim.a
order by vh_src.a, vh_dim.a;

-- 8.2 Derived KNN + FULL JOIN
select 'hnsw_full_knn_derived' as tag;
select knn.a k_a, knn.dist, vh_dim.a d_a, vh_dim.label
from (
    select a, l2_distance(b, cast('[1,2,3]' as vecf32(3))) dist
    from vh_src
    order by dist
    limit 3
) knn
full outer join vh_dim on knn.a = vh_dim.a
order by knn.a, vh_dim.a;

set experimental_ivf_index = 0;
set experimental_hnsw_index = 0;

-- -------------------------------------------------------------
-- 9. Non-sys tenant (regular account) coverage
--
-- Cluster tables and the system database mo_catalog are owned by
-- the sys tenant, but regular tenants must still be able to use
-- FULL OUTER JOIN against normal / partitioned / indexed tables.
-- This section creates a fresh account and exercises the main
-- join shapes from within that account, including a negative test
-- that confirms a non-sys tenant cannot create a cluster table.
-- -------------------------------------------------------------
drop account if exists fulljoin_acc;
create account fulljoin_acc admin_name = 'fj_admin' identified by '111';

-- @session:id=2&user=fulljoin_acc:fj_admin&password=111
drop database if exists fulljoin_acc_db;
create database fulljoin_acc_db;
use fulljoin_acc_db;

-- 9.1 Basic FULL OUTER JOIN in a non-sys tenant
create table na_a(id int primary key, v varchar(5));
create table na_b(id int primary key, v varchar(5));
insert into na_a values (1,'a1'),(2,'a2'),(3,'a3');
insert into na_b values (2,'b2'),(3,'b3'),(4,'b4');

select 'na_basic_full' as tag;
select na_a.id, na_a.v, na_b.id, na_b.v
from na_a full outer join na_b on na_a.id = na_b.id
order by coalesce(na_a.id, na_b.id);

-- 9.2 Multi-column ON in a non-sys tenant
create table na_o(oid int, uid int, amt decimal(10,2));
create table na_p(oid int, uid int, paid decimal(10,2));
insert into na_o values (1,100,50.00),(2,100,25.00),(3,200,10.00),(4,300,5.00);
insert into na_p values (1,100,50.00),(2,100,20.00),(5,400,8.00);
select 'na_multi_on' as tag;
select na_o.oid, na_o.uid, na_o.amt, na_p.oid, na_p.uid, na_p.paid
from na_o full outer join na_p
  on na_o.oid = na_p.oid and na_o.uid = na_p.uid
order by coalesce(na_o.oid, na_p.oid);

-- 9.3 WHERE anti-left / anti-right in a non-sys tenant
select 'na_anti_left' as tag;
select na_a.id, na_a.v from na_a full outer join na_b on na_a.id = na_b.id
where na_b.id is null order by na_a.id;

select 'na_anti_right' as tag;
select na_b.id, na_b.v from na_a full outer join na_b on na_a.id = na_b.id
where na_a.id is null order by na_b.id;

-- 9.4 Partition table (HASH) + FULL in non-sys tenant
create table na_ph(a int, v varchar(5)) partition by hash(a) partitions 4;
insert into na_ph values (1,'a'),(2,'b'),(5,'c'),(7,'d'),(NULL,'n');
create table na_ref(a int, label varchar(5));
insert into na_ref values (1,'R1'),(3,'R3'),(15,'R15');

select 'na_part_full' as tag;
select na_ph.a, na_ph.v, na_ref.a, na_ref.label
from na_ph full outer join na_ref on na_ph.a = na_ref.a
order by coalesce(na_ph.a, na_ref.a), na_ph.v;

-- 9.5 IVFFLAT vector index + FULL in non-sys tenant
set experimental_ivf_index = 1;
create table na_v(id int primary key, e vecf32(3));
insert into na_v values (1,'[1,2,3]'),(2,'[1,2,4]'),(3,'[1,3,5]');
create index na_v_idx using ivfflat on na_v(e) lists = 2 op_type 'vector_l2_ops';
create table na_vd(id int primary key, label varchar(5));
insert into na_vd values (2,'X'),(3,'Y'),(9,'Z');

select 'na_ivf_full' as tag;
select na_v.id, na_vd.id, na_vd.label
from na_v full outer join na_vd on na_v.id = na_vd.id
order by coalesce(na_v.id, na_vd.id);

-- 9.6 FULLTEXT-indexed table + FULL (no MATCH) in non-sys tenant
create table na_ft(id int primary key, body varchar(100), fulltext key ftx(body));
insert into na_ft values (1,'hello world'),(2,'red sky'),(3,'blue ocean');

select 'na_ft_full' as tag;
select na_ft.id, na_ft.body, na_vd.id, na_vd.label
from na_ft full outer join na_vd on na_ft.id = na_vd.id
order by coalesce(na_ft.id, na_vd.id);

-- 9.7 CTE + FULL in non-sys tenant
select 'na_cte_full' as tag;
with nc as (select 1 as id union select 3 union select 5)
select nc.id, na_b.id, na_b.v
from nc full outer join na_b on nc.id = na_b.id
order by coalesce(nc.id, na_b.id);

-- 9.8 Privilege negative test: non-sys tenant must NOT be allowed
-- to create a cluster table.
create cluster table na_cluster(a int, b int);

set experimental_ivf_index = 0;
drop database fulljoin_acc_db;
-- @session

drop account if exists fulljoin_acc;

-- -------------------------------------------------------------
-- 10. Mix: partitioned + indexed + view + CTE in one FULL JOIN chain
-- -------------------------------------------------------------
select 'mix_chain' as tag;
with candidates as (
    select a, v from pr where a < 20
)
select candidates.a c_a,
       ref.a       r_a,
       vi_dim.id   v_id,
       vi_dim.label
from candidates
     full outer join ref    on candidates.a = ref.a
     full outer join vi_dim on coalesce(candidates.a, ref.a) = vi_dim.id
order by c_a, r_a, v_id;

-- -------------------------------------------------------------
-- Cleanup
-- -------------------------------------------------------------
drop table if exists vh_src;
drop table if exists vh_dim;
drop table if exists vi_src;
drop table if exists vi_dim;
drop table if exists ft_src;
drop table if exists ft_dim;
drop table if exists ph;
drop table if exists pr;
drop table if exists pk;
drop table if exists pl;
drop table if exists ref;
drop table if exists dims;
drop table if exists normal_t;
drop table if exists fj_peer;
drop database if exists fulljoin_tbl_db;
