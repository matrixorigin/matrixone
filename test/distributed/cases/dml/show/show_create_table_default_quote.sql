-- SHOW CREATE TABLE prints text column defaults once-escaped so replaying the
-- printed statement recreates the same stored defaults, instead of escaping
-- the stored SQL text a second time. Regression for quote/backtick/backslash
-- defaults (issue 31066).
drop database if exists show_create_table_default_quote;
create database show_create_table_default_quote;
use show_create_table_default_quote;
create table t (
  a varchar(32) default 'it''s',
  b varchar(32) default 'x`y',
  c varchar(32) default '',
  d varchar(32) default 'a\\b',
  e varchar(32) default 'plain',
  f int default 9,
  g varchar(32)
);
show create table t;
insert into t(f) values (1);
select a, b, c, d, e, f, g from t;
-- replay the printed SHOW CREATE TABLE output; the defaults must not change
create table replay (`a` varchar(32) DEFAULT 'it''s', `b` varchar(32) DEFAULT 'x`y', `c` varchar(32) DEFAULT '', `d` varchar(32) DEFAULT 'a\\b', `e` varchar(32) DEFAULT 'plain', `f` int DEFAULT 9, `g` varchar(32) DEFAULT NULL);
show create table replay;
insert into replay(f) values (1);
select a, b, c, d, e, f, g from replay;
-- the catalog default text is the same for both tables, not re-escaped
select column_name, column_default, hex(column_default) from information_schema.columns
  where table_schema='show_create_table_default_quote' and table_name='t'
  order by ordinal_position;
select column_name, column_default, hex(column_default) from information_schema.columns
  where table_schema='show_create_table_default_quote' and table_name='replay'
  order by ordinal_position;
drop database show_create_table_default_quote;
