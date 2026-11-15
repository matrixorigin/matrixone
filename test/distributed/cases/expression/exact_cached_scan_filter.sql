-- Exact cached filtering and the owned-vector fallback have the same SQL result.
drop database if exists exact_cached_scan_filter;
create database exact_cached_scan_filter;
use exact_cached_scan_filter;
create table t(id int, d date, q decimal(15,2), k char(1), payload varchar(32));
insert into t values (1,'1994-01-01',0.05,'A','one'),(2,'1994-06-30',null,'A','two'),(3,'1995-01-01',-0.03,'B','three');
-- In-memory workspace uses the ordinary evaluator.
select id from t where d >= date '1994-01-01' and d < date '1995-01-01' and q between 0.04 and 0.06 order by id;
-- Selected scan output must preserve grouped NULL/count and duplicate join rows.
select k,sum(q),count(*) from t where id <= 2 group by k order by k;
select a.id,b.id from t a join t b on a.k=b.k where a.id>1 order by a.id,b.id;

-- Persist a minimal block so the exact cached-column path is exercised.
-- @ignore:0
select mo_ctl('dn','flush','exact_cached_scan_filter.t');
-- Selected scan output must preserve grouped NULL/count and duplicate join rows.
select k,sum(q),count(*) from t where id <= 2 group by k order by k;
select a.id,b.id from t a join t b on a.k=b.k where a.id>1 order by a.id,b.id;

select id from t where d >= date '1994-01-01' and d < date '1995-01-01' and q between 0.04 and 0.06 order by id;
select id from t where id >= 0 order by id;
select id from t where q < -1.00 order by id;
-- OR and IS NULL keep the generic evaluator and its three-valued truth rules.
select id from t where q is null or q < 0.00 order by id;
-- Tombstones must be applied before residual filtering in physical coordinates.
delete from t where id=1;
select id from t where d >= date '1994-01-01' and d < date '1995-01-01' and q between 0.04 and 0.06 order by id;
select id from t where id >= 0 order by id;
drop database exact_cached_scan_filter;
