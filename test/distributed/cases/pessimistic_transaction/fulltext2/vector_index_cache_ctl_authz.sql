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

drop account if exists ivfcache_tnt;
