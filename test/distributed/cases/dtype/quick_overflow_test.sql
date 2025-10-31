-- Quick test for overflow fix
-- Expected: All should ERROR with "data out of range"

-- Test 1: BIGINT addition overflow
SELECT 9223372036854775807 + 1 AS bigint_overflow;

-- Test 2: BIGINT subtraction underflow  
SELECT -9223372036854775808 - 1 AS bigint_underflow;

-- Test 3: INT addition overflow
SELECT 2147483647 + 1 AS int_overflow;

-- Test 4: INT subtraction underflow
SELECT -2147483648 - 1 AS int_underflow;

-- Test 5: UNSIGNED overflow
SELECT CAST(18446744073709551615 AS UNSIGNED) + 1 AS unsigned_overflow;

-- Test 6: Normal addition (should work)
SELECT 100 + 200 AS normal_add;

-- Test 7: Negative addition (should work)
SELECT -100 + (-200) AS negative_add;

-- Test 8: Zero cases (should work)
SELECT 9223372036854775807 + 0 AS max_plus_zero;
SELECT 0 + 0 AS zero_plus_zero;

