-- Proves the sys-admin gate on the vector/fulltext2 index-cache mo_ctl commands
-- (SetVectorIndexFreshnessInterval / GetVectorIndexCacheInfo / EvictVectorIndexCache /
-- GetVectorIndexCacheKeys) is enforced by the frontend privilege layer for EVERY executable
-- placement of mo_ctl -- not just a direct SELECT projection. hasMoCtrl scans every plan node's
-- executable expressions and the gate runs before the object-type branch, so a call hidden in a
-- WHERE filter, a JOIN condition, or a DML WHERE (including a tableless SELECT) cannot bypass it.
-- Only the sys account moadmin role may run mo_ctl (verifyAccountCanExecMoCtrl); an ordinary tenant
-- and the sys account on a non-moadmin role are refused at the SQL layer before any CN broadcast.
drop account if exists ivfcache_tnt;
create account ivfcache_tnt admin_name 'a1' identified by '111';

-- Ordinary tenant (its accountadmin, NOT sys/moadmin): refused in every executable position.
-- @session:id=1&user=ivfcache_tnt:a1&password=111{
create database ivfcache_tdb;
use ivfcache_tdb;
create table t(id int primary key);
insert into t values (1),(2);
-- direct projection
select mo_ctl('cn', 'GetVectorIndexCacheKeys', '');
-- WHERE filter (tableless) — the reviewer's bypass shape
select 1 where mo_ctl('cn', 'GetVectorIndexCacheKeys', '') is not null;
-- JOIN condition
select 1 from (select 1 x) a join (select 1 y) b on mo_ctl('cn', 'EvictVectorIndexCache', 'victim') is not null;
-- DML WHERE
delete from t where mo_ctl('cn', 'EvictVectorIndexCache', 'victim') is not null;
-- window function: mo_ctl hidden inside a window spec (Expr_W), which the pre-fix gate did not
-- traverse -- MAX(mo_ctl(...)) OVER () and mo_ctl in the OVER order-by must both be refused.
select max(mo_ctl('cn', 'GetVectorIndexCacheKeys', '')) over () as x from t;
select id, row_number() over (order by mo_ctl('cn', 'EvictVectorIndexCache', 'victim')) as rn from t;
-- prepared SET: EXECUTE evaluates the bound value expression directly (no synthetic SELECT), which
-- the pre-fix gate did not scan -- both PREPARE and EXECUTE must be refused.
prepare pctl from "set @v = mo_ctl('cn', 'GetVectorIndexCacheKeys', '')";
execute pctl;
-- window frame bound: a RANGE INTERVAL frame expression is EVALUATED during binding (before the
-- frontend gate runs) and then replaced by a constant, so no scan of the built plan can ever see
-- the call. It is rejected at bind time as an illegal frame bound, so the cluster-wide side effect
-- never fires for an ordinary tenant (#28985).
create table wf(a datetime, b int);
insert into wf values ('2020-01-01 00:00:00', 1);
select a, sum(b) over (order by a range interval mo_ctl('cn', 'SetVectorIndexFreshnessInterval', '1s') day preceding) from wf;
-- @session}

-- Sys account but a non-moadmin role: also refused (projection, filter, window and prepared-SET forms).
set role public;
select mo_ctl('cn', 'GetVectorIndexCacheKeys', '');
select 1 where mo_ctl('cn', 'GetVectorIndexCacheKeys', '') is not null;
select max(mo_ctl('cn', 'GetVectorIndexCacheKeys', '')) over () as x from (select 1) d;
prepare pctl_pub from "set @v = mo_ctl('cn', 'GetVectorIndexCacheKeys', '')";
execute pctl_pub;
set role moadmin;

-- Sys account moadmin (the only authorized principal): allowed in projection, filter, window and
-- prepared-SET forms -- the gate must not over-reject the authorized principal.
select mo_ctl('cn', 'GetVectorIndexCacheKeys', '') like '%GETVECTORINDEXCACHEKEYS%' as allowed_direct;
select 1 as allowed_where where mo_ctl('cn', 'GetVectorIndexCacheKeys', '') is not null;
select (w.x like '%GETVECTORINDEXCACHEKEYS%') as allowed_window from (select max(mo_ctl('cn', 'GetVectorIndexCacheKeys', '')) over () as x from (select 1) d) w;
prepare pctl_ok from "set @v = mo_ctl('cn', 'GetVectorIndexCacheKeys', '')";
execute pctl_ok;
select @v like '%GETVECTORINDEXCACHEKEYS%' as allowed_prepared_set;
deallocate prepare pctl_ok;

-- Defense in depth: mo_ctl in a window frame bound is never a valid frame offset, so it is refused
-- for EVERY principal -- including the authorized sys-account moadmin -- at bind time, closing the
-- bind-time-evaluation bypass regardless of privilege (#28985).
create database if not exists ivfcache_admindb;
use ivfcache_admindb;
create table wf(a datetime, b int);
insert into wf values ('2020-01-01 00:00:00', 1);
select a, sum(b) over (order by a range interval mo_ctl('cn', 'GetVectorIndexCacheKeys', '') day preceding) from wf;
drop database ivfcache_admindb;

drop account if exists ivfcache_tnt;
