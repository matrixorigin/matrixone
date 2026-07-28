-- LCA reconstruction must quote user-defined primary-key column names.

drop database if exists br_lca_quoted_pk;
create database br_lca_quoted_pk;
use br_lca_quoted_pk;

-- DIFF with a reserved-word primary key.
create table diff_base(`select` int primary key, payload varchar(20));
insert into diff_base values (1, 'one'), (2, 'two');
data branch create table diff_left from diff_base;
data branch create table diff_right from diff_base;
delete from diff_left where `select` = 1;
update diff_right set payload = 'two-right' where `select` = 2;
data branch diff diff_left against diff_right;

-- DIFF with reserved-word and punctuation composite primary-key columns.
create table composite_base(
  `left` int,
  `a-b` varchar(10),
  payload varchar(20),
  primary key(`left`, `a-b`)
);
insert into composite_base values (1, 'x', 'one'), (2, 'y', 'two');
data branch create table composite_left from composite_base;
data branch create table composite_right from composite_base;
delete from composite_left where `left` = 1 and `a-b` = 'x';
update composite_right set payload = 'two-right' where `left` = 2 and `a-b` = 'y';
data branch diff composite_left against composite_right;

-- MERGE uses the same LCA reconstruction path for source-side deletions.
create table merge_base(`select` int primary key, payload varchar(20));
insert into merge_base values (1, 'one'), (2, 'two');
data branch create table merge_src from merge_base;
data branch create table merge_dst from merge_base;
delete from merge_src where `select` = 1;
data branch merge merge_src into merge_dst;
select * from merge_dst order by `select`;

-- PICK uses the same path when applying a selected deletion.
create table pick_base(`select` int primary key, payload varchar(20));
insert into pick_base values (1, 'one'), (2, 'two');
data branch create table pick_src from pick_base;
data branch create table pick_dst from pick_base;
delete from pick_src where `select` = 1;
data branch pick pick_src into pick_dst keys(1);
select * from pick_dst order by `select`;

drop database br_lca_quoted_pk;
