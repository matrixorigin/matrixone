-- Issue #32211: standalone CREATE INDEX on a partitioned table must
-- publish the logical parent index (consumers resolving the logical table
-- id: SHOW INDEX, SHOW CREATE TABLE, index hints) together with the
-- physical partition-child maintenance, so the statement result is the
-- same operation MySQL defines it as: ALTER TABLE ... ADD INDEX. The
-- implicit per-case DB of this harness (issue_32211_partition_create_index)
-- is mo-tester Executor-owned and torn down by the harness itself; the
-- content DB below is private to this batch and dropped exactly. The
-- driver's SQL stage asserts the catalog rows parent/child; this public
-- case pins the statement flow through real consumers, including the
-- indexed-key query path after DML maintenance. Expected .result is a
-- semantic derivation and stays NOT_RUN until the authorized single
-- foreground run byte-compares it (baseline must visibly FAIL here; the
-- fixed binary must match the derived expected bytes exactly).
drop database if exists index32211_bvt;
create database index32211_bvt;
use index32211_bvt;
create table t1 (id int primary key, nm varchar(32), c1 int) partition by range (id) (partition p0 values less than (10), partition p1 values less than (maxvalue));
insert into t1 values (1,'k01',1),(5,'k05',5),(20,'k20',20),(30,'k30',30);
create index idx_nm on t1 (nm);
select id from t1 force index (idx_nm) where nm = 'k20';
select id from t1 where nm = 'k05';
update t1 set nm = 'k20b' where nm = 'k20';
select id from t1 force index (idx_nm) where nm = 'k20b';
select id from t1 where nm = 'k20';
delete from t1 where id in (1, 20);
select id from t1 force index (idx_nm) where nm = 'k30';
create index idx_cpx on t1 (c1 desc);
create unique index uidx_nm on t1 (nm);
create unique index ubt_nm on t1 (nm) using btree;
drop database index32211_bvt;
