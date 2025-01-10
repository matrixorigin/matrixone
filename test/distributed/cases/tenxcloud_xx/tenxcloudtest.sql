-- test starts 
CREATE DATABASE IF NOT EXISTS test;
USE test;
CREATE TABLE IF NOT EXISTS tbl_test (id INT);

-- Select all data from the table (using SQL_NO_CACHE for testing purposes)
SELECT /*!40001 SQL_NO_CACHE */ * FROM `tbl_test`;

-- Cleanup
DROP TABLE IF EXISTS tbl_test;
DROP DATABASE IF EXISTS test;
