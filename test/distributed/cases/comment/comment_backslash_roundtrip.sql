-- SHOW CREATE TABLE must keep backslashes in column and table comments (#31068):
-- replaying its output has to store the same comments it displays.
drop database if exists comment_bs_rt;
create database comment_bs_rt;
use comment_bs_rt;

-- default sql_mode: the source literals use MySQL backslash escapes
create table t1 (a int comment 'C:\\dir\\new', b int comment 'it''s') comment 'x\\ty';
show create table t1;
-- storage decodes the escapes: column a keeps C:\dir\new, the table keeps x\ty
select column_name, hex(column_comment) from information_schema.columns
where table_schema = 'comment_bs_rt' and table_name = 't1' order by column_name;
select hex(table_comment) from information_schema.tables
where table_schema = 'comment_bs_rt' and table_name = 't1';

-- replay the SHOW CREATE TABLE output: the replayed table must store identical comments
create table t2 (
`a` int DEFAULT NULL COMMENT 'C:\\dir\\new',
`b` int DEFAULT NULL COMMENT 'it''s'
) COMMENT='x\\ty';
show create table t2;
select column_name, hex(column_comment) from information_schema.columns
where table_schema = 'comment_bs_rt' and table_name = 't2' order by column_name;
select hex(table_comment) from information_schema.tables
where table_schema = 'comment_bs_rt' and table_name = 't2';
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

-- under NO_BACKSLASH_ESCAPES a backslash is an ordinary character: the comment
-- renders verbatim and replays unchanged. The runner reuses one session for all
-- scripts, so save the incoming sql_mode here and restore it below: later BVT
-- files depend on the default mode (correlated columns in HAVING are only
-- planned under ONLY_FULL_GROUP_BY compat).
set @comment_bs_saved_mode = @@sql_mode;
set sql_mode = 'NO_BACKSLASH_ESCAPES';
create table t3 (a int comment 'L:\dir\new') comment 'L:\ty';
show create table t3;
create table t4 (`a` int DEFAULT NULL COMMENT 'L:\dir\new') COMMENT='L:\ty';
show create table t4;
select column_name, hex(column_comment) from information_schema.columns
where table_schema = 'comment_bs_rt' and table_name = 't4' order by column_name;
select hex(table_comment) from information_schema.tables
where table_schema = 'comment_bs_rt' and table_name = 't4';
set sql_mode = @comment_bs_saved_mode;

-- columns and tables without backslashes keep their plain rendering
create table t5 (a int comment 'plain text') comment 'plain table';
show create table t5;
drop database if exists comment_bs_rt;
