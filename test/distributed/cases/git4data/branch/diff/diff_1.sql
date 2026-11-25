drop database if exists test;
create database test;
use test;

-- case 1: t1 and t2 have no LCA
create table t1(a int, b varchar(10), primary key(a));
insert into t1 values(1, "1"),(2, "2"),(3, "3");
create snapshot sp1 for table test t1;

create table t2 like t1;
insert into t2 values(1, "1"), (2, "2"), (4, "4");
create snapshot sp2 for table test t2;

data branch diff t2{snapshot="sp2"} against t1{snapshot="sp1"};
data branch diff t1{snapshot="sp1"} against t2{snapshot="sp2"};

drop snapshot sp1;
drop snapshot sp2;

-- Give one side an explicit declaration without changing its text storage.
alter table t2 modify column b varchar(10) character set utf8mb4 collate utf8mb4_bin;
create snapshot sp1 for table test t1;
create snapshot sp2 for table test t2;
data branch diff t2{snapshot="sp2"} against t1{snapshot="sp1"} output count;
drop snapshot sp1;
drop snapshot sp2;
drop table t1;
drop table t2;

-- case 2: t1 and t2 have the LCA t0
--  i. t1 and t2 are branched from t0 at the same time.
create table t0(a int, b int, primary key(a));
insert into t0 values(1,1),(2,2),(3,3);
create snapshot sp0 for table test t0;

data branch create table t1 from t0{snapshot="sp0"};
insert into t1 values(4,4);
create snapshot sp1 for table test t1;

data branch create table t2 from t0{snapshot="sp0"};
insert into t2 values(5, 5);
create snapshot sp2 for table test t2;

data branch diff t2{snapshot="sp2"} against t1{snapshot="sp1"};
data branch diff t1{snapshot="sp1"} against t2{snapshot="sp2"};

drop snapshot sp1;
drop snapshot sp2;
drop snapshot sp0;
drop table t0;
drop table t1;
drop table t2;

--  ii. t1 and t2 have different branch time.
create table t0(a int, b int, primary key(a));
insert into t0 values(1,1);
create snapshot sp00 for table test t0;
insert into t0 values(2,2);
create snapshot sp01 for table test t0;

data branch create table t1 from t0{snapshot="sp00"};
insert into t1 values(4,4);
create snapshot sp1 for table test t1;

data branch create table t2 from t0{snapshot="sp01"};
insert into t2 values(5, 5);
create snapshot sp2 for table test t2;

data branch diff t2{snapshot="sp2"} against t1{snapshot="sp1"};
data branch diff t1{snapshot="sp1"} against t2{snapshot="sp2"};

drop snapshot sp1;
drop snapshot sp2;
drop snapshot sp00;
drop snapshot sp01;
drop table t0;
drop table t1;
drop table t2;

-- case 3: t1 is the LCA of t1 and t2
--  i. t1.snapshot < t2.branchTS
create table t1(a int, b int, primary key(a));
insert into t1 values(1,1),(2,2);
create snapshot sp1 for table test t1;
insert into t1 values(3,3);

data branch create table t2 from t1;
insert into t2 values(4, 4);
create snapshot sp2 for table test t2;

data branch diff t2{snapshot="sp2"} against t1{snapshot="sp1"};
data branch diff t1{snapshot="sp1"} against t2{snapshot="sp2"};

drop snapshot sp1;
drop snapshot sp2;
drop table t1;
drop table t2;

--  ii. t1.snapshot == t2.branchTS
create table t1(a int, b int, primary key(a));
insert into t1 values(1,1),(2,2);
create snapshot sp1 for table test t1;
insert into t1 values(3,3);

data branch create table t2 from t1{snapshot="sp1"};
insert into t2 values(4, 4);
create snapshot sp2 for table test t2;

data branch diff t2{snapshot="sp2"} against t1{snapshot="sp1"};
data branch diff t1{snapshot="sp1"} against t2{snapshot="sp2"};

drop snapshot sp1;
drop snapshot sp2;
drop table t1;
drop table t2;

--  iii. t1.snapshot > t2.branchTS
create table t1(a int, b int, primary key(a));
insert into t1 values(1,1),(2,2);

data branch create table t2 from t1;
insert into t2 values(4, 4);
create snapshot sp2 for table test t2;

create snapshot sp1 for table test t1;
insert into t1 values(3,3);

data branch diff t2{snapshot="sp2"} against t1{snapshot="sp1"};
data branch diff t1{snapshot="sp1"} against t2{snapshot="sp2"};

drop snapshot sp1;
drop snapshot sp2;
drop table t1;
drop table t2;

-- Read the inherited row after one branch declares an equivalent text charset.
create table lca_parent(a int primary key, b varchar(10));
insert into lca_parent values(1, 'old');
data branch create table lca_child from lca_parent;
alter table lca_child modify column b varchar(10) character set utf8mb4 collate utf8mb4_bin;
update lca_parent set b = 'new' where a = 1;
data branch diff lca_child against lca_parent;

-- Resolve the historical column when a rename and a charset declaration occur on opposite endpoints.
create table rename_parent(a int primary key, b varchar(10));
insert into rename_parent values(1, 'old');
data branch create table rename_child from rename_parent;
alter table rename_child modify column b varchar(10) character set utf8mb4 collate utf8mb4_bin;
alter table rename_parent rename column b to renamed_b;
update rename_child set b = 'new' where a = 1;
data branch diff rename_child against rename_parent;


-- Probe both branch directions after an explicit charset declaration on the inherited key.
create table charset_pk_parent(k varchar(10) primary key, v int);
insert into charset_pk_parent values ('a',1),('b',2);
data branch create table charset_pk_child from charset_pk_parent;
alter table charset_pk_child modify k varchar(10) character set utf8mb4 collate utf8mb4_bin not null, algorithm=inplace;
update charset_pk_child set v = v + 10;
data branch diff charset_pk_child against charset_pk_parent output count;
data branch diff charset_pk_parent against charset_pk_child output count;
data branch pick charset_pk_child into charset_pk_parent keys('a');
select k, v from charset_pk_parent order by k;
data branch merge charset_pk_child into charset_pk_parent when conflict accept;
select k, v from charset_pk_parent order by k;

-- A text output declaration does not change either generated expression.
create table charset_generated_plain(id int primary key, payload varchar(20));
create table charset_generated_explicit(id int primary key, payload varchar(20));
insert into charset_generated_plain values (1, 'old');
insert into charset_generated_explicit values (1, 'new');
alter table charset_generated_plain add column copied varchar(20) generated always as (payload) stored, add column fixed varchar(20) generated always as ('a') stored;
alter table charset_generated_explicit add column copied varchar(20) character set utf8mb4 collate utf8mb4_bin generated always as (payload) stored, add column fixed varchar(20) character set utf8mb4 collate utf8mb4_bin generated always as ('a') stored;
data branch diff charset_generated_explicit against charset_generated_plain output count;

drop database test;
