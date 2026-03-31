drop database if exists test;
create database test;
use test;

-- ================================================================
-- PICK Test 1: Basic PICK with all LCA topologies
-- ================================================================

-- case 1: no LCA — pick specific rows from independent tables
create table t1 (a int, b int, primary key(a));
insert into t1 values (1,1),(3,3),(5,5);

create table t2 (a int, b int, primary key(a));
insert into t2 values (1,1),(2,2),(4,4);

-- diff to see what exists
data branch diff t2 against t1;

-- pick only pk=2 from t2 into t1
data branch pick t2 into t1 keys(2);
select * from t1 order by a asc;

-- pick pk=4 from t2 into t1
data branch pick t2 into t1 keys(4);
select * from t1 order by a asc;

-- verify diff after picks
data branch diff t2 against t1;

drop table t1;
drop table t2;

-- case 2: has LCA (t0 is the common ancestor)
create table t0 (a int, b int, primary key(a));
insert into t0 values (1,1),(2,2),(3,3);

data branch create table t1 from t0;
insert into t1 values (4,4);

data branch create table t2 from t0;
insert into t2 values (5,5),(6,6),(7,7);

-- diff to see divergence
data branch diff t2 against t1;

-- pick only pk=5 and pk=7 from t2 into t1 (skip pk=6)
data branch pick t2 into t1 keys(5,7);
select * from t1 order by a asc;

-- verify: pk=6 should still be in diff
data branch diff t2 against t1;

drop table t0;
drop table t1;
drop table t2;

-- case 3: t1 is the LCA (dst is ancestor of src)
create table t1 (a int, b int, primary key(a));
insert into t1 values (1,1),(2,2);

data branch create table t2 from t1;
insert into t2 values (3,3),(4,4),(5,5);

-- pick only pk=3 from t2 into t1
data branch pick t2 into t1 keys(3);
select * from t1 order by a asc;

-- verify: pk=4,5 still in diff
data branch diff t2 against t1;

drop table t1;
drop table t2;

-- case 4: t2 is the LCA (src is ancestor of dst)
create table t2 (a int, b int, primary key(a));
insert into t2 values (1,1),(2,2);

data branch create table t1 from t2;
insert into t1 values (3,3),(4,4);

-- t2 has no new data relative to t1, but t1 has new data
-- pick should be a no-op since t2 has nothing new
data branch diff t2 against t1;

-- pick non-existing key: should be a no-op
data branch pick t2 into t1 keys(99);
select * from t1 order by a asc;

drop table t1;
drop table t2;

drop database test;
