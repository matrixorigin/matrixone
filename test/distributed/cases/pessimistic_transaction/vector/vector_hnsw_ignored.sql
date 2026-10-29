-- Rows an HNSW index cannot store (#31493). Each section proves the claim of
-- docs/design/20261028-index-ignored-rows.md it is headed by.
drop database if exists hnsw_ignored;
create database hnsw_ignored;
use hnsw_ignored;

set experimental_hnsw_index = 1;
select mo_ctl('cn', 'SetVectorIndexFreshnessInterval', '1s') not like '%failed=[%' as all_cn_updated;

-- ---------------- I4: a synchronous CREATE INDEX or ADD INDEX fails and leaves no index -----
create table t(id bigint primary key, v vecf32(2));
insert into t values (1,'[1,1]'),(4,'[100000,1]');
create index i using hnsw on t(v) op_type 'vector_l2_ops' quantization 'float16';
alter table t add index i using hnsw (v) op_type 'vector_l2_ops' quantization 'float16';
-- a copying ALTER TABLE that adds the index fails too
alter table t add column c0 int, add index i using hnsw (v) op_type 'vector_l2_ops' quantization 'float16';
show index ignored from t;
select count(*) from mo_catalog.mo_indexes where name = 'i' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = 'hnsw_ignored' and relname = 't');
-- a vecbf16 value beyond the float16 range
create table b(id bigint primary key, v vecbf16(2));
insert into b values (1,'[1,1]'),(2,'[100000,1]');
create index ib using hnsw on b(v) op_type 'vector_l2_ops' quantization 'float16';

-- ---------------- I1: a row that cannot be stored does not hold back later rows -----
delete from t where id = 4;
create index i using hnsw on t(v) op_type 'vector_l2_ops' quantization 'float16';
insert into t values (4,'[100000,1]');
insert into t values (2,'[100000,100000]');
insert into t values (3,'[2,2]');
-- @ignore:3
-- @wait_expect(2, 60)
show index ignored from t;
-- @wait_expect(2, 60)
select id from t order by l2_distance(v,'[2,2]') limit 3 by rank with option 'mode=pre';
select id from t order by l2_distance(v,'[2,2]') limit 3 by rank with option 'mode=force';
-- a vecbf16 value beyond the float16 range, kept by an asynchronous index
create index ib using hnsw on b(v) op_type 'vector_l2_ops' quantization 'float16' async;
insert into b values (3,'[2,2]'),(4,'[1,99999]');
-- @ignore:3
-- @wait_expect(2, 60)
show index ignored from b;
-- @wait_expect(2, 60)
select id from b order by l2_distance(v,'[2,2]') limit 2 by rank with option 'mode=pre';

-- ---------------- I2: the ignored rows follow the source rows -----
-- id 2 becomes storable and is indexed; id 3 becomes unstorable and leaves the index;
-- id 4 is deleted; id 1 changes from one unstorable value to another
update t set v = '[1,99999]' where id = 1;
-- @ignore:3
-- @wait_expect(2, 60)
show index ignored from t where Primary_key = '1';
update t set v = '[1,70000]' where id = 1;
update t set v = '[3,3]' where id = 2;
update t set v = '[1,70000]' where id = 3;
delete from t where id = 4;
-- @ignore:3
-- @wait_expect(2, 60)
show index ignored from t;
-- @wait_expect(2, 60)
select id from t order by l2_distance(v,'[2,2]') limit 3 by rank with option 'mode=pre';

-- ---------------- I3: every other build ignores and records the rows -----
-- REINDEX and REINDEX FORCE_SYNC rebuild them
update t set v = '[1,1]' where id = 1;
alter table t alter reindex i hnsw;
-- @ignore:3
show index ignored from t;
alter table t alter reindex i hnsw force_sync;
-- @ignore:3
show index ignored from t;
-- the ALTER TABLE rebuild
alter table t add column c int;
-- @ignore:3
-- @wait_expect(2, 60)
show index ignored from t;
-- an asynchronous CREATE INDEX
create table a(id bigint primary key, v vecf32(2));
insert into a values (1,'[1,1]'),(2,'[100000,1]');
create index ia using hnsw on a(v) op_type 'vector_l2_ops' quantization 'float16' async;
-- @ignore:3
-- @wait_expect(2, 60)
show index ignored from a;
-- TRUNCATE starts them over
truncate t;
insert into t(id, v) values (9,'[1,99999]'),(10,'[1,1]');
-- @ignore:3
-- @wait_expect(2, 60)
show index ignored from hnsw_ignored.t;

-- ---------------- I5: a search vector the index cannot store is rejected -----
select id from t order by l2_distance(v,'[1,100000]') limit 1;

-- ---------------- I6: SHOW INDEX IGNORED lists every index and partition; WHERE filters -----
create table m(id bigint primary key, v1 vecf32(2), v2 vecf32(2));
insert into m values (4,'[1,1]','[1,1]');
create index m1 using hnsw on m(v1) op_type 'vector_l2_ops' quantization 'float16';
create index m2 using hnsw on m(v2) op_type 'vector_l2_ops' quantization 'float16';
insert into m values (1,'[70000,1]','[1,1]'),(2,'[1,1]','[1,70000]'),(3,'[70000,1]','[1,70000]');
-- @ignore:3
-- @wait_expect(2, 60)
show index ignored from m;
-- @ignore:3
show index ignored from m where Key_name = 'm1';
-- @ignore:3
show index ignored from m where Key_name = 'm2';
-- @ignore:3
show index ignored from m where Primary_key = '3' and Reason like '%F16%';
show index ignored from m where Key_name = 'nope';
-- a change of one column leaves the other index's ignored rows as they are
update m set v1 = '[2,2]' where id = 3;
-- @ignore:3
-- @wait_expect(2, 60)
show index ignored from m where Key_name = 'm1';
-- @ignore:3
show index ignored from m where Key_name = 'm2';
-- a partitioned table keeps an index on each partition
create table p(id bigint primary key, v vecf32(2)) partition by hash(id) partitions 2;
insert into p values (1,'[1,1]');
create index pi using hnsw on p(v) op_type 'vector_l2_ops' quantization 'float16';
insert into p values (2,'[1,70000]'),(3,'[70000,1]');
-- @ignore:3
-- @wait_expect(2, 60)
show index ignored from hnsw_ignored.p;
-- a table without ignored rows, and an unknown table
create table e(a int primary key, b int, key kb(b));
show index ignored from e;
show index ignored from nope;

-- ---------------- I7: SHOW INDEX IGNORED requires SELECT on the table -----
drop role if exists hnsw_ignored_r;
drop user if exists hnsw_ignored_u;
create role hnsw_ignored_r;
grant connect on account * to hnsw_ignored_r;
grant show tables on database hnsw_ignored to hnsw_ignored_r;
create user hnsw_ignored_u identified by '111' default role hnsw_ignored_r;
-- @session:id=2&user=sys:hnsw_ignored_u:hnsw_ignored_r&password=111
show index ignored from hnsw_ignored.m;
show index ignored from hnsw_ignored.e;
-- @session
grant select on table hnsw_ignored.m to hnsw_ignored_r;
-- @session:id=2&user=sys:hnsw_ignored_u:hnsw_ignored_r&password=111
show index ignored from hnsw_ignored.e where exists (select 1 from hnsw_ignored.m);
show index ignored from hnsw_ignored.e where Key_name = (select 'm2' from hnsw_ignored.m limit 1);
-- @session
grant select on table hnsw_ignored.e to hnsw_ignored_r;
-- @session:id=2&user=sys:hnsw_ignored_u:hnsw_ignored_r&password=111
-- @ignore:3
show index ignored from hnsw_ignored.m where Key_name = 'm2';
show index ignored from hnsw_ignored.e;
show index ignored from hnsw_ignored.e where exists (select 1 from hnsw_ignored.m);
-- @session
drop user hnsw_ignored_u;
drop role hnsw_ignored_r;

-- DROP INDEX removes the ignored table with the index
drop index i on t;
show index ignored from t;
select count(*) from mo_catalog.mo_indexes where name = 'i' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = 'hnsw_ignored' and relname = 't');

select mo_ctl('cn', 'SetVectorIndexFreshnessInterval', '0') not like '%failed=[%' as all_cn_restored;
drop database hnsw_ignored;
