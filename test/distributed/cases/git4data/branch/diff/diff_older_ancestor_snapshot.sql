drop snapshot if exists diff_older_ancestor_sp;
drop snapshot if exists diff_same_table_old_sp;
drop snapshot if exists diff_same_table_renamed_sp;
drop database if exists test_diff_older_ancestor;
create database test_diff_older_ancestor;
use test_diff_older_ancestor;

-- Both ancestor and branch writes contribute to the net diff at the old snapshot.
create table t0(id int primary key, v int);
insert into t0 values (1,10),(2,20),(3,30),(4,40),(5,50);
create snapshot diff_older_ancestor_sp for database test_diff_older_ancestor;
update t0 set v = v + 1 where id in (1,3);
delete from t0 where id = 2;
insert into t0 values (6,60),(7,70);
data branch create table t1 from t0;
update t1 set v = 30 where id = 3;
update t1 set v = 41 where id = 4;
delete from t1 where id in (5,7);
insert into t1 values (8,80);
data branch diff t1 against t0{snapshot='diff_older_ancestor_sp'};
data branch diff t1 against t0{snapshot='diff_older_ancestor_sp'} output summary;
data branch diff t0{snapshot='diff_older_ancestor_sp'} against t1;
data branch diff t0{snapshot='diff_older_ancestor_sp'} against t1 output summary;

-- An intermediate branch carries its own changes and the ancestor's changes.
data branch create table t2 from t1;
update t2 set v = 81 where id = 8;
insert into t2 values (9,90);
data branch diff t2 against t0{snapshot='diff_older_ancestor_sp'};

-- The ancestor generation is visible at the old snapshot and changes span COPY ALTER.
drop snapshot diff_older_ancestor_sp;
create table a0(id int primary key, v int);
insert into a0 values (1,10),(2,20),(3,30);
create table n0(id int, v int);
insert into n0 values (1,10),(2,20);
create table c0(id int, region int, v int, primary key(id,region));
insert into c0 values (1,1,10),(1,2,20),(2,1,30);
create snapshot diff_older_ancestor_sp for database test_diff_older_ancestor;
update a0 set v = 11 where id = 1;
alter table a0 add column extra int default 0;
delete from a0 where id = 2;
update a0 set v = 31 where id = 3;
data branch create table a1 from a0;
data branch diff a1 against a0{snapshot='diff_older_ancestor_sp'} columns (id,v);
data branch diff a0{snapshot='diff_older_ancestor_sp'} against a1 columns (id,v);

-- The fake-PK path also resolves the original rows at the older snapshot.
update n0 set v = 11 where id = 1;
delete from n0 where id = 2;
data branch create table n1 from n0;
data branch diff n1 against n0{snapshot='diff_older_ancestor_sp'};

-- Composite keys must resolve tombstones for the full key.
update c0 set v = 11 where id = 1 and region = 1;
delete from c0 where id = 1 and region = 2;
data branch create table c1 from c0;
data branch diff c1 against c0{snapshot='diff_older_ancestor_sp'};
drop snapshot diff_older_ancestor_sp;

-- Resolve deleted rows using the column name visible at each probe timestamp.
create table r0(id int primary key, v int);
insert into r0 values (1,10);
create table m0(id int primary key, v int);
insert into m0 values (1,10);
create table p0(id int primary key, v int);
insert into p0 values (1,10);
create snapshot diff_older_ancestor_sp for database test_diff_older_ancestor;
data branch create table r1 from r0;
alter table r0 rename column v to w;
data branch create table r2 from r0;
delete from r1 where id = 1;
delete from r2 where id = 1;
data branch diff r1 against r0;
data branch diff r2 against r0{snapshot='diff_older_ancestor_sp'};
data branch diff r0{snapshot='diff_older_ancestor_sp'} against r2;

-- The ancestor's current name belonged to another table at the fork.
create table original_name(id int primary key, v int);
create table reused_name(id int primary key, v int);
insert into original_name values (1,10);
insert into reused_name values (1,99);
data branch create table renamed_child from original_name;
rename table reused_name to decoy_name;
rename table original_name to reused_name;
delete from renamed_child where id = 1;
data branch diff renamed_child against reused_name;

-- Same-table DIFF reads the deleted value under its name at the earlier snapshot.
create table s0(id int primary key, v int);
insert into s0 values (1,10);
create snapshot diff_same_table_old_sp for database test_diff_older_ancestor;
alter table s0 rename column v to w;
delete from s0 where id = 1;
create snapshot diff_same_table_renamed_sp for database test_diff_older_ancestor;
data branch diff s0 against s0{snapshot='diff_same_table_old_sp'};
data branch diff s0{snapshot='diff_same_table_old_sp'} against s0;
data branch diff s0{snapshot='diff_same_table_renamed_sp'} against s0{snapshot='diff_same_table_old_sp'};
data branch diff s0{snapshot='diff_same_table_old_sp'} against s0{snapshot='diff_same_table_renamed_sp'};
drop snapshot diff_same_table_renamed_sp;
drop snapshot diff_same_table_old_sp;

-- Sibling PICK uses the name and row inherited at each sibling's own fork.
create table sb0(id int primary key, v int);
insert into sb0 values (1,10);
data branch create table sb1 from sb0;
update sb0 set v = 20 where id = 1;
alter table sb0 rename column v to w;
data branch create table sb2 from sb0;
update sb2 set w = 10 where id = 1;
data branch diff sb2 against sb1;
data branch diff sb1 against sb2;
update sb1 set v = 30 where id = 1;
data branch pick sb2 into sb1 keys(1) when conflict accept;
select * from sb1 order by id;

-- A source DELETE must not overwrite a post-fork replacement in the destination.
data branch create table m1 from m0;
delete from m1 where id = 1;
delete from m0 where id = 1;
insert into m0 values (1,99);
data branch merge m1 into m0{snapshot='diff_older_ancestor_sp'} when conflict fail;
select * from m0 order by id;

-- PICK writes the source insert, then preserves destination-only changes when
-- the source snapshot precedes the fork.
update p0 set v = 11 where id = 1;
data branch create table p1 from p0;
insert into p1 values (2,20);
data branch pick p1 into p0 keys(2);
select * from p0 order by id;
data branch pick p0{snapshot='diff_older_ancestor_sp'} into p1 keys(1) when conflict accept;
select * from p1 order by id;
drop snapshot diff_older_ancestor_sp;
drop database test_diff_older_ancestor;
