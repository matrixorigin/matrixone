drop database if exists test;
create database test;
use test;

-- =====================================================
-- Case 1: basic schema evolution merge - target has extra column
--   base:   [a, b]
--   target: [a, b, c]
-- =====================================================
create table merge_extra_col_base(a int, b int, primary key(a));
insert into merge_extra_col_base values(1,1),(2,2),(3,3);
create snapshot sp0 for table test merge_extra_col_base;

data branch create table merge_extra_col_branch from merge_extra_col_base{snapshot="sp0"};
alter table merge_extra_col_branch add column c int default 0;
update merge_extra_col_branch set c=10 where a=1;
insert into merge_extra_col_branch values(4,4,40);
create snapshot sp1 for table test merge_extra_col_branch;

-- MERGE should succeed: only common columns (a, b) are written to base.
data branch merge merge_extra_col_branch into merge_extra_col_base;

-- Verify: the base gets a=4 inserted, c is not written (the base has no column c).
select * from merge_extra_col_base order by a;

drop snapshot sp1;
drop snapshot sp0;
drop table merge_extra_col_base;
drop table merge_extra_col_branch;

-- =====================================================
-- Case 2: merge with INSERT + UPDATE on common column
--   the branch updates b (common) and adds a new row -> both applied to the base
-- =====================================================
create table merge_common_update_base(a int, b int, primary key(a));
insert into merge_common_update_base values(1,1),(2,2),(3,3);
create snapshot sp0 for table test merge_common_update_base;

data branch create table merge_common_update_branch from merge_common_update_base{snapshot="sp0"};
alter table merge_common_update_branch add column c int default 0;
update merge_common_update_branch set b=99, c=10 where a=1;
insert into merge_common_update_branch values(4,4,40);
create snapshot sp1 for table test merge_common_update_branch;

data branch merge merge_common_update_branch into merge_common_update_base when conflict accept;

-- base: a=1 b updated to 99, a=4 inserted
select * from merge_common_update_base order by a;

drop snapshot sp1;
drop snapshot sp0;
drop table merge_common_update_base;
drop table merge_common_update_branch;

-- =====================================================
-- Case 3: merge with DELETE
--   the branch deletes a row -> merge deletes it from the base
-- =====================================================
create table merge_delete_base(a int, b int, primary key(a));
insert into merge_delete_base values(1,1),(2,2),(3,3);
create snapshot sp0 for table test merge_delete_base;

data branch create table merge_delete_branch from merge_delete_base{snapshot="sp0"};
alter table merge_delete_branch add column c int default 0;
delete from merge_delete_branch where a=2;
create snapshot sp1 for table test merge_delete_branch;

data branch merge merge_delete_branch into merge_delete_base;

-- base: a=2 deleted
select * from merge_delete_base order by a;

drop snapshot sp1;
drop snapshot sp0;
drop table merge_delete_base;
drop table merge_delete_branch;

-- =====================================================
-- Case 4: merge with composite PK + extra column
--   base:   [`select`, `line item`, c]   composite PK with quoted/reserved names
--   target: [`select`, `line item`, c, d]
-- =====================================================
create table merge_composite_pk_base(`select` int, `line item` int, c int, primary key(`select`,`line item`));
insert into merge_composite_pk_base values(1,1,10),(2,2,20);
create snapshot sp0 for table test merge_composite_pk_base;

data branch create table merge_composite_pk_branch from merge_composite_pk_base{snapshot="sp0"};
-- Keep this UPDATE in the pre-ALTER generation so MERGE exercises historical
-- mapping after COPY ALTER rebuilds the hidden composite-key column.
update merge_composite_pk_branch set c=99 where `select`=1 and `line item`=1;
alter table merge_composite_pk_branch add column d int default 0;
insert into merge_composite_pk_branch values(3,3,30,300);
create snapshot sp1 for table test merge_composite_pk_branch;

data branch merge merge_composite_pk_branch into merge_composite_pk_base when conflict accept;

-- base: (1,1) c updated to 99, (3,3) inserted, d is not written
select * from merge_composite_pk_base order by `select`;

drop snapshot sp1;
drop snapshot sp0;
drop table merge_composite_pk_base;
drop table merge_composite_pk_branch;

-- =====================================================
-- Case 5: merge is idempotent - merging twice produces no new changes
-- =====================================================
create table merge_idempotent_base(a int, b int, primary key(a));
insert into merge_idempotent_base values(1,1),(2,2);
create snapshot sp0 for table test merge_idempotent_base;

data branch create table merge_idempotent_branch from merge_idempotent_base{snapshot="sp0"};
alter table merge_idempotent_branch add column c int default 0;
insert into merge_idempotent_branch values(3,3,30);
create snapshot sp1 for table test merge_idempotent_branch;

data branch merge merge_idempotent_branch into merge_idempotent_base;
select * from merge_idempotent_base order by a;

-- Second merge: no diff, no changes
data branch merge merge_idempotent_branch into merge_idempotent_base;
select * from merge_idempotent_base order by a;

drop snapshot sp1;
drop snapshot sp0;
drop table merge_idempotent_base;
drop table merge_idempotent_branch;

-- =====================================================
-- Case 6: target-only column sits between common columns
--   base:   [a, b]
--   target: [a, c, b]
-- =====================================================
create table merge_middle_col_base(a int, b int, primary key(a));
insert into merge_middle_col_base values(1,1),(2,2),(3,3);
create snapshot sp0 for table test merge_middle_col_base;

data branch create table merge_middle_col_branch from merge_middle_col_base{snapshot="sp0"};
alter table merge_middle_col_branch add column c int default 0 after a;
update merge_middle_col_branch set b=99 where a=1;
update merge_middle_col_branch set c=88 where a=2;
insert into merge_middle_col_branch(a,c,b) values(4,40,4);
create snapshot sp1 for table test merge_middle_col_branch;

data branch merge merge_middle_col_branch into merge_middle_col_base when conflict accept;
select * from merge_middle_col_base order by a;

drop snapshot sp1;
drop snapshot sp0;
drop table merge_middle_col_base;
drop table merge_middle_col_branch;

-- =====================================================
-- Case 7: cluster-by + added column reaches the fake-PK rejection boundary
-- =====================================================
-- MatrixOne does not allow an explicit primary key together with CLUSTER BY.
-- A legal cluster-by table therefore uses the fake PK, and adding a target-only
-- column is rejected before MERGE apply can expose any hidden helper column.
create table merge_fake_pk_base(a int, b int) cluster by(a,b);
insert into merge_fake_pk_base values(1,1),(2,2);

data branch create table merge_fake_pk_branch from merge_fake_pk_base;
alter table merge_fake_pk_branch add column c int default 0 after a;

-- @regex("schema compatibility check: target-only columns require an explicit primary key", true)
data branch merge merge_fake_pk_branch into merge_fake_pk_base when conflict accept;

drop table merge_fake_pk_base;
drop table merge_fake_pk_branch;

-- =====================================================
-- Case 8: DML across multiple ALTER generations is preserved
-- =====================================================
create table merge_multi_alter_base(a int primary key, b int);
insert into merge_multi_alter_base values(1,1),(2,2),(3,3);
data branch create table merge_multi_alter_branch from merge_multi_alter_base;
update merge_multi_alter_branch set b=11 where a=1;
alter table merge_multi_alter_branch add column c int default 0;
delete from merge_multi_alter_branch where a=2;
alter table merge_multi_alter_branch add column d varchar(20) default 'x';

data branch merge merge_multi_alter_branch into merge_multi_alter_base;
select * from merge_multi_alter_base order by a;

drop table merge_multi_alter_base;
drop table merge_multi_alter_branch;

-- =====================================================
-- Case 9: MERGE ignores target-only historical type changes and values
-- =====================================================
create table merge_target_type_base(a int primary key, b int);
insert into merge_target_type_base values(1,1),(2,2);
data branch create table merge_target_type_branch from merge_target_type_base;
alter table merge_target_type_branch add column c int default 7;
update merge_target_type_branch set b=11, c=70 where a=1;
alter table merge_target_type_branch modify column c varchar(20);
data branch merge merge_target_type_branch into merge_target_type_base;
select * from merge_target_type_base order by a;
drop table merge_target_type_branch;
drop table merge_target_type_base;

-- =====================================================
-- Case 10: DROP/ADD with the same name cannot replay stale values
-- =====================================================
create table merge_redefined_col_base(a int primary key, b int);
insert into merge_redefined_col_base values(1,1);
data branch create table merge_redefined_col_branch from merge_redefined_col_base;
update merge_redefined_col_branch set b=5 where a=1;
alter table merge_redefined_col_branch drop column b;
alter table merge_redefined_col_branch add column b int default 0;
-- @regex("schema compatibility check: column 'b' has different identity", true)
data branch merge merge_redefined_col_branch into merge_redefined_col_base;
select * from merge_redefined_col_base;
drop table merge_redefined_col_branch;
drop table merge_redefined_col_base;

-- =====================================================
-- Case 11: incompatible target-only LCA columns are not probed
-- =====================================================
create table merge_lca_type_base(a int primary key, b int, c int);
insert into merge_lca_type_base values(1,1,1);
data branch create table merge_lca_type_modified from merge_lca_type_base;
data branch create table merge_lca_type_dropped from merge_lca_type_base;
alter table merge_lca_type_modified modify column c varchar(20);
alter table merge_lca_type_dropped drop column c;
update merge_lca_type_modified set b=11, c='x' where a=1;
data branch merge merge_lca_type_modified into merge_lca_type_dropped;
select * from merge_lca_type_dropped;
drop table merge_lca_type_dropped;
drop table merge_lca_type_modified;
drop table merge_lca_type_base;

drop database test;
