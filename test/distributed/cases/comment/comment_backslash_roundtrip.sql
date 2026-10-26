-- SHOW CREATE TABLE must keep backslashes in column and table comments (#31068).
drop database if exists comment_bs_rt;
create database comment_bs_rt;
use comment_bs_rt;
create table t1 (a int comment 'C:\\dir\\new', b int comment 'it''s') comment 'x\\ty';
show create table t1;
select column_name, hex(column_comment) from information_schema.columns where table_schema = 'comment_bs_rt' and table_name = 't1' and column_name = 'a';
select hex(table_comment) from information_schema.tables where table_schema = 'comment_bs_rt' and table_name = 't1';
-- replay the SHOW CREATE TABLE output
create table t2 (
  `a` int DEFAULT NULL COMMENT 'C:\\dir\\new',
  `b` int DEFAULT NULL COMMENT 'it''s'
) COMMENT='x\\ty';
show create table t2;
select column_name, hex(column_comment) from information_schema.columns where table_schema = 'comment_bs_rt' and table_name = 't2' and column_name = 'a';
select hex(table_comment) from information_schema.tables where table_schema = 'comment_bs_rt' and table_name = 't2';
-- A real full archive produced by main 12eeeb0205 before this fix. Its
-- manifest, hashes and two-row main/index object files are unmodified.
drop stage if exists comment_bs_legacy_stage;
create stage comment_bs_legacy_stage url = 'file://$resources/table_dump_comment_legacy/';
create table legacy_dump_target (id int primary key, v varchar(32) default 'd\\ef', c int comment 'C:\\dir\\new it''s', key idx_v(v) comment 'idx\\q''s') comment 'x\\ty''z';
load table legacy_dump_target from 'stage://comment_bs_legacy_stage/';
select * from legacy_dump_target order by id;
select id from legacy_dump_target force index(idx_v) where v='one';
drop stage comment_bs_legacy_stage;
-- DUMP schema fingerprints retain their historical representation even though
-- the replayable SHOW/CREATE LIKE comments now escape backslashes.
drop stage if exists comment_bs_dump_stage;
create stage comment_bs_dump_stage url = 'file:///tmp/mo-comment-backslash-roundtrip-31068/';
remove files from stage if exists 'stage://comment_bs_dump_stage/metadata/*';
dump table t1 to 'stage://comment_bs_dump_stage/metadata' metadata only;
create table dump_target like t1;
load table dump_target from 'stage://comment_bs_dump_stage/metadata';
select column_name, hex(column_comment) from information_schema.columns where table_schema = 'comment_bs_rt' and table_name = 'dump_target' and column_name = 'a';
select hex(table_comment) from information_schema.tables where table_schema = 'comment_bs_rt' and table_name = 'dump_target';
create table dump_wrong (a int comment 'different', b int comment 'it''s') comment 'x\\ty';
load table dump_wrong from 'stage://comment_bs_dump_stage/metadata';
select count(*) from dump_wrong;
remove files from stage if exists 'stage://comment_bs_dump_stage/metadata/*';
drop stage comment_bs_dump_stage;
drop database comment_bs_rt;
