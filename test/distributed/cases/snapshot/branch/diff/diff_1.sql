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

drop database test;