-- #31747: EXPLAIN FOR CONNECTION is parsed but not implemented. It must
-- report a clean not-supported error instead of an internal error that echoes
-- a mangled "explain format =  for connection" statement.
explain for connection 328;
explain for connection 999;
explain format = 'json' for connection 999;
describe for connection 1;
desc format = 'tree' for connection 2;
-- Ordinary EXPLAIN keeps working.
explain select 1;
