-- Diff correctness after flush + checkpoint + GC.
-- Verifies that data branch diff returns correct results even after
-- partition-state data has been flushed, checkpointed, and garbage-collected.
-- Derived from repro_stale_read.sql and repro_stale_read_2.sql.

drop database if exists test_gc_diff;
create database test_gc_diff;
use test_gc_diff;

-- Case 1: Complex PK table, insert into branch, diff + merge after flush+ckp+gc
-- (from repro_stale_read.sql)
create table c1_src (
  `memory_id` varchar(64) not null,
  `user_id` varchar(64) not null,
  `session_id` varchar(64) default null,
  `memory_type` varchar(20) not null,
  `content` text not null,
  `initial_confidence` float not null,
  `trust_tier` varchar(10) default null,
  `source_event_ids` json not null,
  `superseded_by` varchar(64) default null,
  `is_active` smallint not null default '1',
  `observed_at` datetime(6) not null,
  `created_at` datetime(6) not null,
  `updated_at` datetime(6) default null,
  primary key (`memory_id`)
);

insert into c1_src
  (memory_id, user_id, content, memory_type, trust_tier, is_active,
   initial_confidence, source_event_ids, observed_at, created_at, updated_at)
values ('base-001', 'user1', 'base content', 'semantic', 'T1', 1, 0.9, '[]',
        '2025-01-01 00:00:00.000000', '2025-01-01 00:00:00.000000', '2025-01-01 00:00:00.000000');

-- @ignore:0
select mo_ctl('dn', 'flush', 'test_gc_diff.c1_src');

data branch create table c1_tar from c1_src;

insert into c1_tar
  (memory_id, user_id, content, memory_type, trust_tier, is_active,
   initial_confidence, source_event_ids, observed_at, created_at, updated_at)
values ('test-mem-001', 'test', 'content', 'semantic', 'T2', 1, 0.8, '[]',
        '2025-01-01 00:00:00.000000', '2025-01-01 00:00:00.000000', '2025-01-01 00:00:00.000000');

data branch diff c1_tar against c1_src output summary;

-- @ignore:0
select mo_ctl('dn', 'flush', 'test_gc_diff.c1_tar');
-- @ignore:0
select mo_ctl('dn', 'flush', 'test_gc_diff.c1_src');
-- @ignore:0
select mo_ctl('dn', 'globalcheckpoint', '');
-- @ignore:0
select mo_ctl('dn', 'globalcheckpoint', '');
-- @ignore:0
select mo_ctl('dn', 'diskcleaner', 'force_gc');
-- @ignore:0
select mo_ctl('dn', 'globalcheckpoint', '');
-- @ignore:0
select mo_ctl('dn', 'diskcleaner', 'force_gc');

data branch diff c1_tar against c1_src output summary;
data branch merge c1_tar into c1_src when conflict accept;
select count(*) from c1_src;

drop table c1_src;
drop table c1_tar;

-- Case 2: PK table, 200K rows, update on branch, diff after flush+ckp+gc
-- (from repro_stale_read_2.sql)
create table c2_src (a int primary key, b int);
insert into c2_src select *, * from generate_series(1, 200000) g;

data branch create table c2_tar from c2_src;
update c2_tar set b = b + 1 where a mod 1119 = 0;

data branch diff c2_tar against c2_src output summary;
-- @ignore:0
select count(*) as updated_rows_before_gc from c2_tar where b != a;

-- @ignore:0
select mo_ctl('dn', 'flush', 'test_gc_diff.c2_tar');
-- @ignore:0
select mo_ctl('dn', 'flush', 'test_gc_diff.c2_src');
-- @ignore:0
select mo_ctl('dn', 'globalcheckpoint', '');
-- @ignore:0
select mo_ctl('dn', 'globalcheckpoint', '');
-- @ignore:0
select mo_ctl('dn', 'diskcleaner', 'force_gc');
-- @ignore:0
select mo_ctl('dn', 'globalcheckpoint', '');
-- @ignore:0
select mo_ctl('dn', 'diskcleaner', 'force_gc');

data branch diff c2_tar against c2_src output summary;
data branch diff c2_tar against c2_src output count;
-- @ignore:0
select count(*) as updated_rows_after_gc from c2_tar where b != a;

drop table c2_src;
drop table c2_tar;

-- Case 3: No-PK (fake PK) table, 200K rows, update on branch, diff after flush+ckp+gc
create table c3_src (a int, b int);
insert into c3_src select *, * from generate_series(1, 200000) g;

data branch create table c3_tar from c3_src;
update c3_tar set b = b + 1 where a mod 1119 = 0;

data branch diff c3_tar against c3_src output summary;

-- @ignore:0
select mo_ctl('dn', 'flush', 'test_gc_diff.c3_tar');
-- @ignore:0
select mo_ctl('dn', 'flush', 'test_gc_diff.c3_src');
-- @ignore:0
select mo_ctl('dn', 'globalcheckpoint', '');
-- @ignore:0
select mo_ctl('dn', 'globalcheckpoint', '');
-- @ignore:0
select mo_ctl('dn', 'diskcleaner', 'force_gc');
-- @ignore:0
select mo_ctl('dn', 'globalcheckpoint', '');
-- @ignore:0
select mo_ctl('dn', 'diskcleaner', 'force_gc');

data branch diff c3_tar against c3_src output summary;
data branch diff c3_tar against c3_src output count;

drop table c3_src;
drop table c3_tar;

drop database test_gc_diff;
