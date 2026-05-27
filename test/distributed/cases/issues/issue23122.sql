-- Issue #23122: Parser support for multi-table ANALYZE TABLE, CHECK TABLE, and SHOW PROFILE

-- =============================================
-- Test 1: ANALYZE TABLE single table (backward compatibility)
-- =============================================

drop database if exists `__issue_23122_test__`;
create database `__issue_23122_test__`;
use `__issue_23122_test__`;

drop table if exists t1;
create table t1 (a int, b int);
insert into t1 values (1, 2), (3, 4);

analyze table t1(a, b);

-- =============================================
-- Test 2: ANALYZE TABLE multi-table
-- =============================================

drop table if exists t2;
create table t2 (c int, d int);
insert into t2 values (5, 6), (7, 8);

analyze table t1(a, b), t2(c, d);

-- =============================================
-- Test 3: CHECK TABLE basic (expects "not supported" error)
-- =============================================

check table t1;

-- =============================================
-- Test 4: CHECK TABLE EXTENDED (expects "not supported" error)
-- =============================================

check table t1 extended;

-- =============================================
-- Test 5: CHECK TABLE multiple tables (expects "not supported" error)
-- =============================================

check table t1, t2;

-- =============================================
-- Test 6: CHECK TABLE FOR UPGRADE (expects "not supported" error)
-- =============================================

check table t1 for upgrade;

-- =============================================
-- Test 7: SHOW PROFILE basic (expects "not supported" error)
-- =============================================

show profile;

-- =============================================
-- Test 8: SHOW PROFILE FOR QUERY (expects "not supported" error)
-- =============================================

show profile for query 1;

-- =============================================
-- Test 9: SHOW PROFILE with LIMIT (expects "not supported" error)
-- =============================================

show profile limit 5;

-- =============================================
-- Test 10: SHOW PROFILE FOR QUERY with LIMIT (expects "not supported" error)
-- =============================================

show profile for query 1 limit 5;

-- =============================================
-- Regression: SHOW PROFILES (plural) still works
-- =============================================

show profiles;

-- =============================================
-- Regression: CHECK in CREATE TABLE still works
-- =============================================

drop table if exists t_check;
create table t_check (a int, check (a > 0));

-- =============================================
-- Cleanup
-- =============================================

drop table t1;
drop table t2;
drop table t_check;
drop database `__issue_23122_test__`;
