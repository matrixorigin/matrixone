-- Data branch edge cases that are not covered by basic diff/merge/pick flows.

-- Case 1: secondary/unique indexes remain valid after PICK/MERGE.
drop database if exists br_index;
create database br_index;
use br_index;

create table base(
  id int primary key,
  u int,
  k int,
  v varchar(20),
  unique key uk_u(u),
  key idx_k(k)
);
insert into base values
  (1, 101, 10, 'one'),
  (2, 102, 20, 'two'),
  (3, 103, 30, 'three');

data branch create table dst from base;
data branch create table src from base;

insert into src values (4, 104, 20, 'four');
update src set k = 40, v = 'two-src' where id = 2;
data branch pick src into dst keys(2, 4) when conflict accept;

select id, u, k, v from dst where k = 20 order by id;
select id, u, k, v from dst where k = 40 order by id;

-- Ordinary unique-index enforcement still rejects duplicates after PICK.
insert into dst values (5, 104, 50, 'dup-u');

data branch create table merge_dst from base;
data branch create table merge_src from base;
insert into merge_src values (4, 104, 40, 'four');
data branch merge merge_src into merge_dst;
select id, u, k, v from merge_dst where u = 104;

data branch create table conflict_dst from base;
data branch create table conflict_src from base;
insert into conflict_dst values (4, 104, 40, 'dst-four');
insert into conflict_src values (5, 104, 50, 'src-five');
-- MERGE must not bypass unique-index validation.
data branch merge conflict_src into conflict_dst when conflict accept;
select id, u, k, v from conflict_dst order by id;

drop database br_index;

-- Case 2: schema drift after branching.
drop database if exists br_schema_drift;
create database br_schema_drift;
use br_schema_drift;

create table base(a int primary key, b int);
insert into base values (1, 10), (2, 20), (3, 30);

data branch create table left_add from base;
data branch create table right_add from base;
alter table left_add add column c varchar(20) default 'left-only';
update left_add set b = 11, c = 'changed' where a = 1;
insert into right_add values (4, 40);
data branch diff left_add against right_add columns (a, b) output summary;

data branch create table both_add_left from base;
data branch create table both_add_right from base;
alter table both_add_left add column c varchar(20) default 'same';
alter table both_add_right add column c varchar(20) default 'same';
update both_add_left set c = 'left' where a = 2;
update both_add_right set c = 'right' where a = 3;
data branch diff both_add_left against both_add_right columns (a, c);

data branch create table drop_left from base;
data branch create table drop_right from base;
alter table drop_left drop column b;
-- Divergent schemas are rejected with a schema-equivalence error.
data branch diff drop_left against drop_right;

data branch create table rename_left from base;
data branch create table rename_right from base;
alter table rename_left rename column b to bb;
-- A rename on one branch keeps the renamed table queryable for diff.
data branch diff rename_left against rename_right;

drop database br_schema_drift;

-- Case 3: transaction behavior.
drop database if exists br_txn_src;
drop database if exists br_txn_dst;
create database br_txn_src;
use br_txn_src;

create table base(a int primary key, b varchar(20));
insert into base values (1, 'one'), (2, 'two');

begin;
data branch create table br_commit from base;
commit;
select count(*) as br_commit_rows from br_commit;

begin;
data branch create table br_rollback from base;
rollback;
select count(*) as br_rollback_tables
  from mo_catalog.mo_tables
 where reldatabase = 'br_txn_src' and relname = 'br_rollback';

begin;
data branch delete table br_txn_src.br_commit;
rollback;
select count(*) as br_commit_after_rollback
  from mo_catalog.mo_tables
 where reldatabase = 'br_txn_src' and relname = 'br_commit';

begin;
data branch delete table br_txn_src.br_commit;
commit;
select count(*) as br_commit_after_commit
  from mo_catalog.mo_tables
 where reldatabase = 'br_txn_src' and relname = 'br_commit';

data branch create table pick_dst from base;
data branch create table pick_src from base;
insert into pick_src values (3, 'three');

begin;
data branch pick pick_src into pick_dst keys(3) when conflict accept;
rollback;
select count(*) as pick_dst_rows from pick_dst;

data branch create database br_txn_dst from br_txn_src;
select count(*) as dst_tables
  from mo_catalog.mo_tables
 where reldatabase = 'br_txn_dst'
   and relname in ('base', 'pick_dst', 'pick_src');

begin;
data branch delete database br_txn_dst;
rollback;
select count(*) as dst_tables_after_rollback
  from mo_catalog.mo_tables
 where reldatabase = 'br_txn_dst'
   and relname in ('base', 'pick_dst', 'pick_src');

begin;
data branch delete database br_txn_dst;
commit;
select count(*) as dst_tables_after_commit
  from mo_catalog.mo_tables
 where reldatabase = 'br_txn_dst';

drop database if exists br_txn_dst;
drop database br_txn_src;
