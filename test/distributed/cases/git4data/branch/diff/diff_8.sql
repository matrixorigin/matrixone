drop database if exists test_summary;
create database test_summary;
use test_summary;

-- Case 1: Same table, different snapshots, insert-only delta.
-- Verify output summary can separate target/base counts even when table names are identical.
create table c1(a int primary key, b int);
insert into c1 values (1, 10), (2, 20);
drop snapshot if exists c1_sp0;
create snapshot c1_sp0 for table test_summary c1;
insert into c1 values (3, 30), (4, 40);
drop snapshot if exists c1_sp1;
create snapshot c1_sp1 for table test_summary c1;

data branch diff c1{snapshot="c1_sp1"} against c1{snapshot="c1_sp0"} output summary;
data branch diff c1{snapshot="c1_sp1"} against c1{snapshot="c1_sp0"} output count;
data branch diff c1{snapshot="c1_sp1"} against c1{snapshot="c1_sp0"};

data branch diff c1{snapshot="c1_sp0"} against c1{snapshot="c1_sp1"} output summary;
data branch diff c1{snapshot="c1_sp0"} against c1{snapshot="c1_sp1"} output count;

drop snapshot c1_sp0;
drop snapshot c1_sp1;
drop table c1;

-- Case 2: Same table, different snapshots, mixed update/delete/insert on PK table.
-- Verify summary contains UPDATED and both-side metrics.
create table c2(a int primary key, b int);
insert into c2 values (1, 10), (2, 20), (3, 30);
drop snapshot if exists c2_sp0;
create snapshot c2_sp0 for table test_summary c2;

update c2 set b = 200 where a = 2;
delete from c2 where a = 3;
insert into c2 values (4, 40);

drop snapshot if exists c2_sp1;
create snapshot c2_sp1 for table test_summary c2;

data branch diff c2{snapshot="c2_sp1"} against c2{snapshot="c2_sp0"} output summary;
data branch diff c2{snapshot="c2_sp1"} against c2{snapshot="c2_sp0"} output count;
data branch diff c2{snapshot="c2_sp1"} against c2{snapshot="c2_sp0"};

drop snapshot c2_sp0;
drop snapshot c2_sp1;
drop table c2;

-- Case 3: Data branch created from snapshot (PK table), then changed.
-- Verify summary works with branch lineage and includes UPDATE/DELETE/INSERT style deltas.
create table c3_base(a int primary key, b int);
insert into c3_base values (1, 1), (2, 2), (3, 3);
drop snapshot if exists c3_sp0;
create snapshot c3_sp0 for table test_summary c3_base;

data branch create table c3_tar from c3_base{snapshot="c3_sp0"};
update c3_tar set b = b + 100 where a = 1;
delete from c3_tar where a = 2;
insert into c3_tar values (4, 4);

data branch diff c3_tar against c3_base{snapshot="c3_sp0"} output summary;
data branch diff c3_tar against c3_base{snapshot="c3_sp0"} output count;
data branch diff c3_tar against c3_base{snapshot="c3_sp0"};

data branch diff c3_base{snapshot="c3_sp0"} against c3_tar output summary;
data branch diff c3_base{snapshot="c3_sp0"} against c3_tar output count;

drop snapshot c3_sp0;
drop table c3_base;
drop table c3_tar;

-- Case 4: Data branch on table without PK.
-- Verify summary is still stable on fake PK path.
create table c4_base(a int, b int);
insert into c4_base values (1, 10), (2, 20), (3, 30);
drop snapshot if exists c4_sp0;
create snapshot c4_sp0 for table test_summary c4_base;

data branch create table c4_tar from c4_base{snapshot="c4_sp0"};
update c4_tar set b = b + 100 where a = 1;
delete from c4_tar where a = 2;
insert into c4_tar values (5, 50);

data branch diff c4_tar against c4_base{snapshot="c4_sp0"} output summary;
data branch diff c4_tar against c4_base{snapshot="c4_sp0"} output count;
data branch diff c4_tar against c4_base{snapshot="c4_sp0"};

drop snapshot c4_sp0;
drop table c4_base;
drop table c4_tar;

-- Case 5: Two branches diverge from the same snapshot.
-- Verify summary target/base columns can both be non-zero in one query.
create table c5_seed(a int primary key, b int);
insert into c5_seed values (1, 10), (2, 20), (3, 30), (4, 40);
drop snapshot if exists c5_sp0;
create snapshot c5_sp0 for table test_summary c5_seed;

data branch create table c5_left from c5_seed{snapshot="c5_sp0"};
data branch create table c5_right from c5_seed{snapshot="c5_sp0"};

update c5_left set b = b + 100 where a in (1, 2);
delete from c5_left where a = 4;
insert into c5_left values (5, 50);

update c5_right set b = b + 200 where a in (2, 3);
delete from c5_right where a = 1;
insert into c5_right values (6, 60);

data branch diff c5_left against c5_right output summary;
data branch diff c5_left against c5_right output count;
data branch diff c5_left against c5_right;

data branch diff c5_right against c5_left output summary;
data branch diff c5_right against c5_left output count;

drop snapshot c5_sp0;
drop table c5_seed;
drop table c5_left;
drop table c5_right;

-- Case 6: Compatibility regression check for old syntax.
-- Verify output count remains usable after introducing output summary.
create table c6(a int primary key, b int);
insert into c6 values (1, 1), (2, 2);
drop snapshot if exists c6_sp0;
create snapshot c6_sp0 for table test_summary c6;
update c6 set b = b + 10 where a = 1;
insert into c6 values (3, 3);

data branch diff c6 against c6{snapshot="c6_sp0"} output count;
data branch diff c6 against c6{snapshot="c6_sp0"} output summary;

drop snapshot c6_sp0;
drop table c6;

drop database test_summary;
