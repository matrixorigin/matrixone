-- Proves the sys-admin gate on the vector/fulltext2 index-cache mo_ctl commands
-- (SetVectorIndexFreshnessInterval / GetVectorIndexCacheInfo / EvictVectorIndexCache /
-- GetVectorIndexCacheKeys) is enforced by the frontend privilege layer: mo_ctl runs only for the
-- sys account's moadmin role (verifyAccountCanExecMoCtrl = IsSysTenant() && IsMoAdminRole()). All
-- four commands are therefore unreachable by an ordinary tenant, or by the sys account on a
-- non-moadmin role -- rejected at the SQL layer before they can broadcast to any CN. This is why the
-- handlers need no additional per-caller account check of their own.
drop account if exists ivfcache_tnt;
create account ivfcache_tnt admin_name 'a1' identified by '111';

-- Ordinary tenant (its accountadmin, which is NOT sys/moadmin): every command is refused.
-- @session:id=1&user=ivfcache_tnt:a1&password=111{
select mo_ctl('cn', 'GetVectorIndexCacheKeys', '');
select mo_ctl('cn', 'GetVectorIndexCacheInfo', 'x');
select mo_ctl('cn', 'SetVectorIndexFreshnessInterval', '2s');
select mo_ctl('cn', 'EvictVectorIndexCache', 'x');
-- @session}

-- Sys account but a non-moadmin role: also refused.
set role public;
select mo_ctl('cn', 'GetVectorIndexCacheKeys', '');
set role moadmin;

-- Sys account moadmin (the only authorized principal): allowed -- returns the command's JSON result.
select mo_ctl('cn', 'GetVectorIndexCacheKeys', '') like '%GETVECTORINDEXCACHEKEYS%' as allowed_for_moadmin;

drop account if exists ivfcache_tnt;
