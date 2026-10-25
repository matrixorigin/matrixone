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
drop database comment_bs_rt;
