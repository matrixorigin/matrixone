-- Rows an HNSW index cannot store (#31493). A synchronous CREATE INDEX rejects them; every other
-- path leaves them out of the index and records them, and the index keeps syncing.
-- SHOW INDEX IGNORED lists them with the reason.
drop database if exists hnsw_ignored;
create database hnsw_ignored;
use hnsw_ignored;

set experimental_hnsw_index = 1;
select mo_ctl('cn', 'SetVectorIndexFreshnessInterval', '1s') not like '%failed=[%' as all_cn_updated;

-- CREATE INDEX fails and leaves no index
create table t(id bigint primary key, v vecf32(2));
insert into t values (1,'[1,1]'),(4,'[100000,1]');
create index i using hnsw on t(v) op_type 'vector_l2_ops' quantization 'float16';
show index ignored from t;
select count(*) from mo_catalog.mo_indexes where name = 'i' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = 'hnsw_ignored' and relname = 't');

-- sync: a row that cannot be stored does not hold back later rows
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

-- an update to a storable value indexes the row; an update to a value that cannot be stored
-- removes it from the index; a delete removes it from the ignored rows
update t set v = '[3,3]' where id = 2;
update t set v = '[1,70000]' where id = 3;
delete from t where id = 4;
-- @ignore:3
-- @wait_expect(2, 60)
show index ignored from t;
-- @wait_expect(2, 60)
select id from t order by l2_distance(v,'[2,2]') limit 3 by rank with option 'mode=pre';

-- REINDEX rebuilds the index and its ignored rows
alter table t alter reindex i hnsw;
-- @ignore:3
show index ignored from t;
select id from t order by l2_distance(v,'[2,2]') limit 3 by rank with option 'mode=pre';

-- TRUNCATE empties them
truncate t;
insert into t values (9,'[1,99999]'),(10,'[1,1]');
-- @ignore:3
-- @wait_expect(2, 60)
show index ignored from hnsw_ignored.t;

-- a partitioned table keeps an index on each partition
create table p(id bigint primary key, v vecf32(2)) partition by hash(id) partitions 2;
insert into p values (1,'[1,1]'),(2,'[1,70000]');
create index pi using hnsw on p(v) op_type 'vector_l2_ops' quantization 'float16';
delete from p where id = 2;
create index pi using hnsw on p(v) op_type 'vector_l2_ops' quantization 'float16';
insert into p values (2,'[1,70000]'),(3,'[70000,1]');
-- @ignore:3
-- @wait_expect(2, 60)
show index ignored from hnsw_ignored.p;

-- each index of a table records its own ignored rows, and WHERE filters them
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
-- a change of one column leaves the other index's ignored rows as they are
update m set v1 = '[2,2]' where id = 3;
-- @ignore:3
-- @wait_expect(2, 60)
show index ignored from m where Key_name = 'm1';
-- @ignore:3
show index ignored from m where Key_name = 'm2';
show index ignored from m where Key_name = 'nope';

-- a table without ignored rows, and an unknown table
create table e(a int primary key, b int, key kb(b));
show index ignored from e;
show index ignored from nope;

drop index i on t;
show index ignored from t;
select count(*) from mo_catalog.mo_indexes where name = 'i' and table_id in (select rel_id from mo_catalog.mo_tables where reldatabase = 'hnsw_ignored' and relname = 't');

select mo_ctl('cn', 'SetVectorIndexFreshnessInterval', '0') not like '%failed=[%' as all_cn_restored;
drop database hnsw_ignored;
