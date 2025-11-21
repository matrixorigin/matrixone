-- Test account to account_id conversion for statement_info and metric tables
-- This test verifies that account column filters are automatically converted to account_id filters
-- for compatibility with mo-cloud's business-level usage of the account field.

-- ============================================
-- Tests for statement_info table
-- ============================================
use system;

-- Test 1: Unqualified column name (account = 'sys')
-- Should convert to account_id = 0
-- @separator:table
EXPLAIN SELECT * FROM statement_info WHERE account = 'sys';

-- Test 2: Table alias qualified column name (s.account = 'sys')
-- Should convert to s.account_id = 0
-- @separator:table
EXPLAIN SELECT * FROM statement_info s WHERE s.account = 'sys';

-- Test 3: Full table name qualified column name (statement_info.account = 'sys')
-- Should convert to statement_info.account_id = 0
-- @separator:table
EXPLAIN SELECT * FROM statement_info WHERE statement_info.account = 'sys';

-- Test 4: Multiple conditions with account filter
-- Should convert account = 'sys' to account_id = 0
-- @separator:table
EXPLAIN SELECT * FROM statement_info WHERE account = 'sys' AND statement_id != '';

-- Test 5: Account IN clause
-- Should convert to account_id IN (SELECT ...)
-- @separator:table
EXPLAIN SELECT * FROM statement_info WHERE account IN ('sys', 'test');

-- Test 6: Account LIKE clause
-- Should convert to account_id IN (SELECT ...)
-- @separator:table
EXPLAIN SELECT * FROM statement_info WHERE account LIKE 'sys%';

-- Test 7: Complex expression with account
-- Should convert account = 'sys' to account_id = 0
-- @separator:table
EXPLAIN SELECT * FROM statement_info WHERE (account = 'sys' OR account = 'test') AND statement_id != '';

-- Test 8: Table alias with different alias name
-- Should convert si.account = 'sys' to si.account_id = 0
-- @separator:table
EXPLAIN SELECT * FROM statement_info si WHERE si.account = 'sys';

-- Test 9: Case insensitive 'sys' optimization
-- Should convert account = 'SYS' to account_id = 0 (case insensitive)
-- @separator:table
EXPLAIN SELECT * FROM statement_info WHERE account = 'SYS';

-- ============================================
-- Tests for system_metrics.metric table
-- ============================================
use system_metrics;

-- Test 1: Unqualified column name (account = 'sys')
-- Should convert to account_id = 0
-- @separator:table
EXPLAIN SELECT * FROM metric WHERE account = 'sys';

-- Test 2: Table alias qualified column name (m.account = 'sys')
-- Should convert to m.account_id = 0
-- @separator:table
EXPLAIN SELECT * FROM metric m WHERE m.account = 'sys';

-- Test 3: Full table name qualified column name (metric.account = 'sys')
-- Should convert to metric.account_id = 0
-- @separator:table
EXPLAIN SELECT * FROM metric WHERE metric.account = 'sys';

-- Test 4: Multiple conditions with account filter
-- Should convert account = 'sys' to account_id = 0
-- @separator:table
EXPLAIN SELECT * FROM metric WHERE account = 'sys' AND metric_name != '';

-- Test 5: Account IN clause
-- Should convert to account_id IN (SELECT ...)
-- @separator:table
EXPLAIN SELECT * FROM metric WHERE account IN ('sys', 'test');

-- Test 6: Account LIKE clause
-- Should convert to account_id IN (SELECT ...)
-- @separator:table
EXPLAIN SELECT * FROM metric WHERE account LIKE 'sys%';

-- Test 7: Complex expression with account
-- Should convert account = 'sys' to account_id = 0
-- @separator:table
EXPLAIN SELECT * FROM metric WHERE (account = 'sys' OR account = 'test') AND metric_name != '';

-- Test 8: Table alias with different alias name
-- Should convert mi.account = 'sys' to mi.account_id = 0
-- @separator:table
EXPLAIN SELECT * FROM metric mi WHERE mi.account = 'sys';

-- Test 9: Case insensitive 'sys' optimization
-- Should convert account = 'SYS' to account_id = 0 (case insensitive)
-- @separator:table
EXPLAIN SELECT * FROM metric WHERE account = 'SYS';



