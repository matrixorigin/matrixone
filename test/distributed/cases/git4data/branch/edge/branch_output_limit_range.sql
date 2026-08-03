-- DATA BRANCH DIFF OUTPUT LIMIT accepts signed limits and rejects unsigned-only values.

-- MaxInt64 reaches semantic validation, proving that it parsed successfully.
-- @regex("table \"issue_26038_missing_a\" does not exist",true)
data branch diff issue_26038_missing_a against issue_26038_missing_b
  output limit 9223372036854775807;

-- Values represented by the lexer as uint64 must return a parser error, not panic.
-- @regex("OUTPUT LIMIT is out of range",true)
data branch diff issue_26038_missing_a against issue_26038_missing_b
  output limit 9223372036854775808;

-- MaxUint64 exercises the upper boundary of the lexer's uint64 representation.
-- @regex("OUTPUT LIMIT is out of range",true)
data branch diff issue_26038_missing_a against issue_26038_missing_b
  output limit 18446744073709551615;
