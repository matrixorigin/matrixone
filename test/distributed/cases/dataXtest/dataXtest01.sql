-- Check if DATABASE test exists
DROP DATABASE IF EXISTS test;

-- test starts 
CREATE DATABASE test;
USE test;
CREATE TABLE t1 (retry INT);
SELECT retry FROM t1;
-- test ends

-- Cleanup
DROP TABLE IF EXISTS t1;
DROP DATABASE test;
