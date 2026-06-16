-- REPLACE INTO must keep a synchronous ivfflat index consistent, the same way
-- INSERT does: a row inserted or whose vector is changed via REPLACE has to be
-- immediately reflected in an index-ordered ANN/KNN query.
-- lists=2 with probe_limit>=lists probes every centroid, so results are
-- deterministic.

drop database if exists vec_ivf_replace;
create database vec_ivf_replace;
use vec_ivf_replace;

set probe_limit=2;

create table t(id int primary key, b vecf32(3));
insert into t values(1,'[1,1,1]'),(2,'[2,2,2]'),(3,'[3,3,3]'),(4,'[4,4,4]'),(5,'[5,5,5]');
create index idx using ivfflat on t(b) lists=2 op_type 'vector_l2_ops';

-- REPLACE a brand-new PK row (= insert): it must be searchable immediately.
replace into t values(7,'[5,5,6]');
select id from t order by l2_distance(b,'[5,5,5]') limit 3;

-- Plain INSERT of a brand-new row, for comparison.
insert into t values(8,'[5,4,5]');
select id from t order by l2_distance(b,'[5,5,5]') limit 4;

-- REPLACE an existing row's vector: the KNN must use the NEW vector, the stale
-- indexed vector must be gone.
replace into t values(1,'[5,5,4]');
select id from t where id=1;
-- id=1 is now far from [1,1,1], so it must NOT come back as the nearest there.
select id from t order by l2_distance(b,'[1,1,1]') limit 3;
-- id=1 is now near [5,5,5], so it must show up there.
select id from t order by l2_distance(b,'[5,5,5]') limit 5;

-- Multi-row REPLACE mixing new and existing rows.
replace into t values(2,'[5,5,3]'),(9,'[5,6,5]'),(3,'[0,0,0]');
select id from t order by l2_distance(b,'[5,5,5]') limit 6;
-- id=3 is now [0,0,0]; the stale [3,3,3] entry must be gone.
select id from t order by l2_distance(b,'[3,3,3]') limit 3;

drop database if exists vec_ivf_replace;
