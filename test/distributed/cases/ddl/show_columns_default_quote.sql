-- SHOW COLUMNS / DESC Default must show the decoded default value (MySQL),
-- not a re-escaped SQL literal. Regression for quote/backslash/backtick defaults.
drop database if exists show_columns_default_quote;
create database show_columns_default_quote;
use show_columns_default_quote;
create table t (
  a varchar(32) default 'it''s',
  b varchar(32) default '',
  c varchar(32) default 'n',
  d varchar(32) default 'x`y',
  e varchar(32) default 'a\\b',
  f int default null,
  g int default 9,
  h varchar(32)
);
show columns from t;
desc t;
select column_name, column_default, hex(column_default) from information_schema.columns
  where table_schema='show_columns_default_quote' and table_name='t'
  order by ordinal_position;
drop database show_columns_default_quote;
