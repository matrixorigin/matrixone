-- #31261: ordinary (non-Hive, read-only) external table SHOW CREATE keeps its
-- FILEPATH so the emitted DDL is re-executable, and the replayed table reads
-- the same rows in another database.
drop database if exists m3a;
create database m3a;
use m3a;
create external table et_src (a int, b varchar(16)) infile{'filepath'='$resources/external_table_file/m3_replay.csv','format'='csv'};
-- @regex("CREATE EXTERNAL TABLE `et_src`", true)
-- @regex("`a` int DEFAULT NULL", true)
-- @regex("`b` varchar\(16\) COLLATE utf8mb4_general_ci DEFAULT NULL", true)
-- @regex("FILEPATH'='[^']*external_table_file/m3_replay\.csv'", true)
-- @regex("'FORMAT'='csv'", true)
show create table et_src;
select * from et_src;
drop database if exists m3a2;
create database m3a2;
use m3a2;
-- replay the emitted DDL text (same infile options) into a second database
create external table et_dst (a int, b varchar(16)) infile{'filepath'='$resources/external_table_file/m3_replay.csv','format'='csv'};
select * from et_dst;
drop database m3a;
drop database m3a2;
