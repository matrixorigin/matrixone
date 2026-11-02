-- Exact cached filtering and the owned-vector fallback have the same SQL result.
drop database if exists exact_cached_scan_filter;
create database exact_cached_scan_filter;
use exact_cached_scan_filter;
create table t(id int, d date, q decimal(15,2));
insert into t values (1,'1994-01-01',0.05),(2,'1994-06-30',null),(3,'1995-01-01',-0.03);
-- In-memory workspace uses the ordinary evaluator.
select id from t where d >= date '1994-01-01' and d < date '1995-01-01' and q between 0.04 and 0.06 order by id;
-- Persist a minimal block so the exact cached-column path is exercised.
-- @ignore:0
select mo_ctl('dn','flush','exact_cached_scan_filter.t');
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
