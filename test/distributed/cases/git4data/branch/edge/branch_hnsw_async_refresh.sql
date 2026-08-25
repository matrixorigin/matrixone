-- HNSW's asynchronous reindex rewrites the source table's catalog row. The
-- rewrite must not move the Data Branch creation boundary past branch DML.
set experimental_hnsw_index = 1;
select enable_fault_injection();

drop database if exists issue27457_hnsw_table;
create database issue27457_hnsw_table;
use issue27457_hnsw_table;

-- Table branches: prove the failure's timing edge and retain a non-HNSW
-- vector control over the same wait.
create table merge_base (id bigint primary key, v vecf32(3));
insert into merge_base values (1, '[1,1,1]'), (2, '[4,4,4]');
create index ix_merge using hnsw on merge_base(v)
  op_type 'vector_l2_ops' max_index_capacity 1000;
select add_fault_point('fj/cdc/executor', ':::', 'echo', 0, 'processInitSQLNewTxn');
data branch create table merge_leaf from merge_base;

create table plain_base (id bigint primary key, v vecf32(3));
insert into plain_base values (1, '[1,1,1]'), (2, '[4,4,4]');
data branch create table plain_leaf from plain_base;

update merge_leaf set v = '[10,10,10]' where id = 1;
insert into merge_leaf values (4, '[0,0,0]');
update plain_leaf set v = '[10,10,10]' where id = 1;
insert into plain_leaf values (4, '[0,0,0]');

data branch diff merge_leaf against merge_base output count;
data branch diff plain_leaf against plain_base output count;

-- Capture the branch catalog version only after its DML. The fault above
-- prevents the asynchronous InitSQL from rewriting the catalog before this
-- point; releasing it makes the ordering under test deterministic.
set @merge_leaf_version = (
  select rel_version from mo_catalog.mo_tables
  where reldatabase = database() and relname = 'merge_leaf'
);
-- disable/enable broadcasts to every CN, releasing the worker regardless of
-- which CN owns this ISCP job and resetting the fault map for the next case.
select disable_fault_injection();
select enable_fault_injection();

-- Observe the exact catalog rewrite that used to replace the creation CTS.
-- @wait_expect(1, 30)
select count(*) from mo_catalog.mo_tables
where reldatabase = database() and relname = 'merge_leaf'
  and rel_version > @merge_leaf_version;

data branch diff merge_leaf against merge_base output count;
data branch diff plain_leaf against plain_base output count;
data branch merge merge_leaf into merge_base when conflict accept;
select id from merge_base order by id;
select count(*) from merge_base
where id = 1 and l2_distance(v, '[10,10,10]') = 0;
select count(*) from merge_base
where id = 4 and l2_distance(v, '[0,0,0]') = 0;

-- PICK must use the same stable creation boundary while applying only the
-- requested updated and inserted keys.
create table pick_base (id bigint primary key, v vecf32(3));
insert into pick_base values (1, '[1,1,1]'), (2, '[4,4,4]');
create index ix_pick using hnsw on pick_base(v)
  op_type 'vector_l2_ops' max_index_capacity 1000;
select add_fault_point('fj/cdc/executor', ':::', 'echo', 0, 'processInitSQLNewTxn');
data branch create table pick_leaf from pick_base;
update pick_leaf set v = '[10,10,10]' where id = 1;
insert into pick_leaf values (4, '[0,0,0]');
data branch diff pick_leaf against pick_base output count;
set @pick_leaf_version = (
  select rel_version from mo_catalog.mo_tables
  where reldatabase = database() and relname = 'pick_leaf'
);
select disable_fault_injection();
select enable_fault_injection();

-- @wait_expect(1, 30)
select count(*) from mo_catalog.mo_tables
where reldatabase = database() and relname = 'pick_leaf'
  and rel_version > @pick_leaf_version;

data branch diff pick_leaf against pick_base output count;
data branch pick pick_leaf into pick_base keys(1, 4);
select id from pick_base order by id;
select count(*) from pick_base
where id = 1 and l2_distance(v, '[10,10,10]') = 0;
select count(*) from pick_base
where id = 4 and l2_distance(v, '[0,0,0]') = 0;

drop database issue27457_hnsw_table;

-- Database branches clone the same indexed table through a distinct frontend
-- path; qualified DIFF and MERGE must retain the same logical history.
drop database if exists issue27457_hnsw_db_src;
drop database if exists issue27457_hnsw_db_leaf;
create database issue27457_hnsw_db_src;
create table issue27457_hnsw_db_src.t (id bigint primary key, v vecf32(3));
insert into issue27457_hnsw_db_src.t values (1, '[1,1,1]'), (2, '[4,4,4]');
create index ix_db using hnsw on issue27457_hnsw_db_src.t(v)
  op_type 'vector_l2_ops' max_index_capacity 1000;
select add_fault_point('fj/cdc/executor', ':::', 'echo', 0, 'processInitSQLNewTxn');
data branch create database issue27457_hnsw_db_leaf from issue27457_hnsw_db_src;
update issue27457_hnsw_db_leaf.t set v = '[10,10,10]' where id = 1;
insert into issue27457_hnsw_db_leaf.t values (4, '[0,0,0]');
data branch diff issue27457_hnsw_db_leaf.t against issue27457_hnsw_db_src.t output count;
set @db_leaf_version = (
  select rel_version from mo_catalog.mo_tables
  where reldatabase = 'issue27457_hnsw_db_leaf' and relname = 't'
);
select disable_fault_injection();

-- @wait_expect(1, 30)
select count(*) from mo_catalog.mo_tables
where reldatabase = 'issue27457_hnsw_db_leaf' and relname = 't'
  and rel_version > @db_leaf_version;

data branch diff issue27457_hnsw_db_leaf.t against issue27457_hnsw_db_src.t output count;
data branch merge issue27457_hnsw_db_leaf.t into issue27457_hnsw_db_src.t when conflict accept;
select id from issue27457_hnsw_db_src.t order by id;
select count(*) from issue27457_hnsw_db_src.t
where id = 1 and l2_distance(v, '[10,10,10]') = 0;
select count(*) from issue27457_hnsw_db_src.t
where id = 4 and l2_distance(v, '[0,0,0]') = 0;

drop database issue27457_hnsw_db_leaf;
drop database issue27457_hnsw_db_src;
set experimental_hnsw_index = 0;
