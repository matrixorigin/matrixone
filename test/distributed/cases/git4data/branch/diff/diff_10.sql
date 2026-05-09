-- Diff correctness via snapshots after tables are dropped.
-- Verifies that data branch diff works using snapshot references when
-- the base and target tables no longer exist in the current state.
-- Each case uses its own database to avoid interference.

-- Case 1: PK table, update on branch, snapshot both sides, drop, diff via snapshots
drop database if exists sp_diff_c1;
create database sp_diff_c1;
use sp_diff_c1;

create table c1_base (a int primary key, b int);
insert into c1_base select *, * from generate_series(1, 100) g;
-- @ignore:0
select mo_ctl('dn', 'flush', 'sp_diff_c1.c1_base');

create snapshot sp_c1_base for account sys;

data branch create table c1_tar from c1_base;
update c1_tar set b = b + 1 where a mod 7 = 0;
-- @ignore:0
select mo_ctl('dn', 'flush', 'sp_diff_c1.c1_tar');

create snapshot sp_c1_tar for account sys;

-- Verify diff before drop
data branch diff c1_tar against c1_base output summary;

-- Drop the tables then diff via snapshots
drop table c1_tar;
drop table c1_base;

data branch diff c1_tar {snapshot = sp_c1_tar} against c1_base {snapshot = sp_c1_base} output summary;

drop snapshot sp_c1_tar;
drop snapshot sp_c1_base;
drop database sp_diff_c1;

-- Case 2: PK table, insert + delete on branch
drop database if exists sp_diff_c2;
create database sp_diff_c2;
use sp_diff_c2;

create table c2_base (a int primary key, b int);
insert into c2_base select *, * from generate_series(1, 100) g;
-- @ignore:0
select mo_ctl('dn', 'flush', 'sp_diff_c2.c2_base');

create snapshot sp_c2_base for account sys;

data branch create table c2_tar from c2_base;
insert into c2_tar values (101, 101), (102, 102), (103, 103);
delete from c2_tar where a <= 5;
-- @ignore:0
select mo_ctl('dn', 'flush', 'sp_diff_c2.c2_tar');

create snapshot sp_c2_tar for account sys;

data branch diff c2_tar against c2_base output summary;

drop table c2_tar;
drop table c2_base;

data branch diff c2_tar {snapshot = sp_c2_tar} against c2_base {snapshot = sp_c2_base} output summary;

drop snapshot sp_c2_tar;
drop snapshot sp_c2_base;
drop database sp_diff_c2;

-- Case 3: PK table, mixed insert + update + delete
drop database if exists sp_diff_c3;
create database sp_diff_c3;
use sp_diff_c3;

create table c3_base (a int primary key, b int);
insert into c3_base select *, * from generate_series(1, 100) g;
-- @ignore:0
select mo_ctl('dn', 'flush', 'sp_diff_c3.c3_base');

create snapshot sp_c3_base for account sys;

data branch create table c3_tar from c3_base;
update c3_tar set b = b + 1 where a mod 7 = 0;
insert into c3_tar values (101, 101), (102, 102);
delete from c3_tar where a <= 3;
-- @ignore:0
select mo_ctl('dn', 'flush', 'sp_diff_c3.c3_tar');

create snapshot sp_c3_tar for account sys;

data branch diff c3_tar against c3_base output summary;

drop table c3_tar;
drop table c3_base;

data branch diff c3_tar {snapshot = sp_c3_tar} against c3_base {snapshot = sp_c3_base} output summary;

drop snapshot sp_c3_tar;
drop snapshot sp_c3_base;
drop database sp_diff_c3;

-- Case 4: No-PK table, update on branch
drop database if exists sp_diff_c4;
create database sp_diff_c4;
use sp_diff_c4;

create table c4_base (a int, b int);
insert into c4_base select *, * from generate_series(1, 100) g;
-- @ignore:0
select mo_ctl('dn', 'flush', 'sp_diff_c4.c4_base');

create snapshot sp_c4_base for account sys;

data branch create table c4_tar from c4_base;
update c4_tar set b = b + 1 where a mod 7 = 0;
-- @ignore:0
select mo_ctl('dn', 'flush', 'sp_diff_c4.c4_tar');

create snapshot sp_c4_tar for account sys;

data branch diff c4_tar against c4_base output summary;

drop table c4_tar;
drop table c4_base;

data branch diff c4_tar {snapshot = sp_c4_tar} against c4_base {snapshot = sp_c4_base} output summary;

drop snapshot sp_c4_tar;
drop snapshot sp_c4_base;
drop database sp_diff_c4;
