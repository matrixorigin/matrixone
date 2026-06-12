-- ivfflat int8 QUANTIZATION across the DDL / maintenance matrix:
-- reindex, table clone, snapshot + db clone, alter table, drop index/table.
-- Two well-separated clusters [1..] and [50..]; queries probe each side and
-- must keep returning the right cluster after every operation (the int8 codes
-- and trained bounds must survive each path, not silently fall back to a raw
-- identity cast).
drop database if exists ivfqddl;
create database ivfqddl;
use ivfqddl;

create table q(a int primary key, v vecf32(4));
insert into q values (1,'[1,1,1,1]'),(2,'[3,3,3,3]'),(3,'[5,5,5,5]'),(4,'[50,50,50,50]'),(5,'[52,52,52,52]'),(6,'[54,54,54,54]');
create index qi8 using ivfflat on q(v) lists=2 op_type 'vector_l2_ops' quantization 'int8';

-- baseline: query near each cluster
select a from q order by l2_distance(v,'[1,1,1,1]') limit 3;
select a from q order by l2_distance(v,'[54,54,54,54]') limit 3;

-- 1) ALTER REINDEX: sync rebuild re-applies the quantizer + re-trains bounds.
alter table q alter reindex qi8 ivfflat lists=2;
select a from q order by l2_distance(v,'[1,1,1,1]') limit 3;
select a from q order by l2_distance(v,'[54,54,54,54]') limit 3;

-- 2) CLONE table: block-level physical copy of entries/centroids/metadata.
create table qc clone q;
select a from qc order by l2_distance(v,'[1,1,1,1]') limit 3;
select a from qc order by l2_distance(v,'[54,54,54,54]') limit 3;

-- 3) SNAPSHOT + db clone-from-snapshot: RestoreTable path (empty seed,
--    block clone, FORCE_SYNC reindex InitSQL).
drop snapshot if exists ivfqsp;
create snapshot ivfqsp for account sys;
drop database if exists ivfqddl2;
create database ivfqddl2 clone ivfqddl {snapshot='ivfqsp'};
select a from ivfqddl2.q order by l2_distance(v,'[1,1,1,1]') limit 3;
select a from ivfqddl2.q order by l2_distance(v,'[54,54,54,54]') limit 3;
drop snapshot ivfqsp;
drop database ivfqddl2;

-- 4) ALTER TABLE add a non-vector column: index must keep working.
alter table q add column note varchar(10) default 'x';
select a from q order by l2_distance(v,'[1,1,1,1]') limit 3;

-- 5) DROP INDEX then recreate int8 on the same column.
alter table q drop index qi8;
create index qi8b using ivfflat on q(v) lists=2 op_type 'vector_l2_ops' quantization 'int8';
select a from q order by l2_distance(v,'[1,1,1,1]') limit 3;
select a from q order by l2_distance(v,'[54,54,54,54]') limit 3;

-- 6) DROP TABLE (clone first, then base).
drop table qc;
drop table q;

drop database ivfqddl;
