-- @suite
-- @case
-- @desc: issue #26549 - historical ALTER lineage must not impersonate a live data branch
-- @label:bvt

drop snapshot if exists issue26549_table_snapshot;
drop snapshot if exists issue26549_rollback_snapshot;
drop snapshot if exists issue26549_database_snapshot;
drop pitr if exists issue26549_table_pitr;
drop database if exists issue26549_alter_txn;
create database issue26549_alter_txn;
use issue26549_alter_txn;

-- A table snapshot makes the first COPY ALTER preserve physical history.  The
-- second ALTER in the same BEGIN transaction must treat that generated
-- level='alter' row as historical lineage, not as a logical data branch.
create table snapshot_alter (
    id varchar(36) primary key,
    status tinyint not null default 1,
    index idx_status (status)
);
insert into snapshot_alter values ('r-001', 1), ('r-002', 1), ('r-003', 1);
create snapshot issue26549_table_snapshot for table issue26549_alter_txn snapshot_alter;
begin;
insert into snapshot_alter values ('r-txn', 1);
alter table snapshot_alter add column a int not null default 0;
alter table snapshot_alter add column b int not null default 0;
commit;
insert into snapshot_alter (id, status) values ('r-new', 1);
select count(*) as indexed_count from snapshot_alter where status = 1;
select count(*) as base_scan_count from snapshot_alter where status + 0 = 1;
select id, status, a, b from snapshot_alter order by id;
select id, status from snapshot_alter {snapshot = 'issue26549_table_snapshot'} order by id;
drop snapshot issue26549_table_snapshot;

-- Exercise the other explicit-transaction form and different COPY ALTER
-- actions while PITR is the only historical owner.
create table pitr_alter (
    id int primary key,
    payload int,
    index idx_payload (payload)
);
insert into pitr_alter values (1, 10), (2, 20);
create pitr issue26549_table_pitr for table issue26549_alter_txn pitr_alter range 1 'h';
set autocommit = 0;
update pitr_alter set payload = 11 where id = 1;
alter table pitr_alter add column extra int not null default 5;
alter table pitr_alter modify column extra bigint not null default 5;
alter table pitr_alter rename column payload to score;
commit;
set autocommit = 1;
insert into pitr_alter values (3, 30, 7);
select id, score, extra from pitr_alter order by id;
select count(*) as renamed_index_count from pitr_alter where score = 20;
select count(*) as renamed_base_count from pitr_alter where score + 0 = 20;
drop pitr issue26549_table_pitr;

-- Repeated ALTER remains fully transactional: rollback must remove both new
-- physical generations and their schema changes.
create table rollback_alter (id int primary key, payload int);
insert into rollback_alter values (1, 10);
create snapshot issue26549_rollback_snapshot for table issue26549_alter_txn rollback_alter;
begin;
alter table rollback_alter add column rolled_back_a int default 1;
alter table rollback_alter add column rolled_back_b int default 2;
rollback;
select column_name
  from information_schema.columns
 where table_schema = 'issue26549_alter_txn' and table_name = 'rollback_alter'
 order by ordinal_position;
select * from rollback_alter order by id;
drop snapshot issue26549_rollback_snapshot;

-- A database-level snapshot covers multiple ordinary tables. Interleaving the
-- first and second ALTERs proves lineage classification stays table-local.
create table multi_alter_a (id int primary key);
create table multi_alter_b (id int primary key);
insert into multi_alter_a values (1);
insert into multi_alter_b values (2);
create snapshot issue26549_database_snapshot for database issue26549_alter_txn;
begin;
alter table multi_alter_a add column a1 int default 11;
alter table multi_alter_b add column b1 int default 21;
alter table multi_alter_a add column a2 int default 12;
alter table multi_alter_b add column b2 int default 22;
commit;
select * from multi_alter_a order by id;
select * from multi_alter_b order by id;
drop snapshot issue26549_database_snapshot;

-- Nearest controls: logical branch ownership remains restricted after ALTER
-- moves either the branch itself or its base to a new physical generation.
create table live_base (id int primary key, payload int);
insert into live_base values (1, 10);
data branch create table live_child from live_base;
alter table live_child add column child_generation int default 1;
begin;
-- @regex("ALTER on a data-branch lineage is not supported inside an explicit transaction", true)
alter table live_child add column rejected_child_generation int default 2;
rollback;
alter table live_base add column base_generation int default 3;
begin;
-- @regex("ALTER on a data-branch lineage is not supported inside an explicit transaction", true)
alter table live_base add column rejected_base_generation int default 4;
rollback;
select id, payload, child_generation from live_child order by id;
select id, payload, base_generation from live_base order by id;
data branch delete table live_child;

drop database issue26549_alter_txn;
