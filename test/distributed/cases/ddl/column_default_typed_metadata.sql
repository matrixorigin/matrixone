-- Issue #31161 (carrying #31159): public column-default metadata renders the
-- typed default value (DECIMAL declared scale, MySQL-compatible BIT literal
-- form) and marks expression defaults DEFAULT_GENERATED on both
-- information_schema.COLUMNS and SHOW COLUMNS, while inserted default values
-- stay unchanged. Literal defaults, explicit NULL defaults and default-less
-- columns keep empty EXTRA.
drop database if exists column_default_typed_metadata;
create database column_default_typed_metadata;
use column_default_typed_metadata;

-- Typed rendering: DECIMAL declared scale and BIT literal form, with literal
-- string / explicit NULL / default-less controls.
create table t_typed (
  id int primary key,
  d decimal(10,4) default 1.2,
  b bit(8) default b'00110011',
  v varchar(10) default 'x',
  n int default null
);
show columns from t_typed;
select column_name, column_default, extra from information_schema.columns
  where table_schema='column_default_typed_metadata' and table_name='t_typed'
  order by ordinal_position;
insert into t_typed(id) values (1);
select id, d, b+0, v, n from t_typed;

-- Expression defaults publish DEFAULT_GENERATED; a folded parenthesized
-- expression keeps its origin text; a rounded literal renders the executed
-- value.
create table t_generated (
  id int primary key,
  ts timestamp(6) null default current_timestamp(6),
  i2 int default (1+2),
  d3 decimal(10,0) default 1.6
);
show columns from t_generated;
select column_name, column_default, extra from information_schema.columns
  where table_schema='column_default_typed_metadata' and table_name='t_generated'
  order by ordinal_position;
insert into t_generated(id) values (1);
select id, ts is not null, i2, d3 from t_generated;

-- Attributed/decimal256 literal variants: unsigned, decimal(40,s),
-- scientific notation and the canonical signed literal all render the typed
-- value with empty EXTRA.
create table t_attributed (
  id int primary key,
  du decimal(10,4) unsigned default 1.2,
  d40 decimal(40,4) default 1.2,
  ds decimal(10,4) unsigned default 1.2e0,
  dp decimal(10,4) unsigned default +1.2,
  db1 decimal(10,4) unsigned default b'00110011'
);
show columns from t_attributed;
select column_name, column_default, extra from information_schema.columns
  where table_schema='column_default_typed_metadata' and table_name='t_attributed'
  order by ordinal_position;
insert into t_attributed(id) values (1);
select id, du, d40, ds, dp, db1 from t_attributed;

-- Integer literals above uint64 bind as decimal128 carriers the attributed
-- admission preserves raw; the typed renderer widens and rescales them like
-- the assignment cast. The uint64-fitting literal, the quoted spelling and
-- the plain (non-attributed) form are the controls.
create table t_u64boundary (
  id int primary key,
  u64max decimal(40,4) unsigned default 18446744073709551615,
  u64p1 decimal(40,4) unsigned default 18446744073709551616,
  u64p1q decimal(40,4) unsigned default '18446744073709551616',
  p64p1 decimal(40,4) default 18446744073709551616,
  bigu decimal(40,4) unsigned default 12345678901234567890.1,
  bign decimal(40,4) default -12345678901234567890.1
);
show columns from t_u64boundary;
select column_name, column_default, extra from information_schema.columns
  where table_schema='column_default_typed_metadata' and table_name='t_u64boundary'
  order by ordinal_position;
insert into t_u64boundary(id) values (1);
select id, u64max, u64p1, u64p1q, p64p1, bigu, bign from t_u64boundary;

-- Explicit-cast and folded-function DECIMAL defaults keep their origin text
-- and publish DEFAULT_GENERATED; the inserted values are the executed
-- results.
create table t_expr (
  id int primary key,
  dc decimal(10,4) unsigned default cast(1.6 as decimal(10,0)),
  da decimal(10,4) default abs(1.2)
);
show columns from t_expr;
select column_name, column_default, extra from information_schema.columns
  where table_schema='column_default_typed_metadata' and table_name='t_expr'
  order by ordinal_position;
insert into t_expr(id) values (1);
select id, dc, da from t_expr;

-- Column-reference defaults publish DEFAULT_GENERATED and store the
-- referenced value; the referenced literal column stays a literal.
create table t_colref (
  id int primary key,
  a int default 7,
  b int default a
);
show columns from t_colref;
select column_name, column_default, extra from information_schema.columns
  where table_schema='column_default_typed_metadata' and table_name='t_colref'
  order by ordinal_position;
insert into t_colref(id) values (1);
select id, a, b from t_colref;

-- Variable-reference defaults publish DEFAULT_GENERATED and store the
-- session value under a fixed time_zone; a quoted variable spelling stays a
-- literal.
set time_zone = '+08:00';
create table t_var (
  id int primary key,
  v varchar(64) default @@time_zone,
  w varchar(64) default (@@time_zone),
  v2 varchar(64) default '@@time_zone'
);
show columns from t_var;
select column_name, column_default, extra from information_schema.columns
  where table_schema='column_default_typed_metadata' and table_name='t_var'
  order by ordinal_position;
insert into t_var(id) values (1);
select id, v, w, v2 from t_var;

-- ALTER ... ADD COLUMN and CREATE TABLE ... LIKE publish the same metadata.
alter table t_typed add column ad decimal(10,4) default 1.2;
alter table t_generated add column ats timestamp(6) null default current_timestamp(6);
create table t_typed_like like t_typed;
show columns from t_typed_like;
select column_name, column_default, extra from information_schema.columns
  where table_schema='column_default_typed_metadata' and table_name='t_typed_like'
  order by ordinal_position;

drop database column_default_typed_metadata;
