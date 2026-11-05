-- #31232 follow-up: escape handling keeps the established mode-specific
-- defaults. For LOAD DATA INLINE an omitted or empty ESCAPED BY leaves
-- escape processing disabled, so a backslash sequence in the payload is
-- plain data; an explicit custom escape byte is honored for field
-- separator protection. The file path keeps its own established default:
-- with ESCAPED BY omitted the reader applies its default backslash escape,
-- so literal-backslash parity between the two modes is asserted with an
-- explicit ESCAPED BY ''.

drop table if exists t31232_inline_esc;
create table t31232_inline_esc(id int primary key, s varchar(40));
load data inline format='csv', data='1,row\\8\n2,a\\nb\n' into table t31232_inline_esc fields terminated by ',';
select * from t31232_inline_esc order by id;
drop table t31232_inline_esc;

-- an explicit empty ESCAPED BY keeps escape processing disabled
drop table if exists t31232_inline_esc_empty;
create table t31232_inline_esc_empty(id int primary key, s varchar(40));
load data inline format='csv', data='1,row\\8\n2,a\\nb\n' into table t31232_inline_esc_empty fields terminated by ',' escaped by '';
select * from t31232_inline_esc_empty order by id;
drop table t31232_inline_esc_empty;

-- an explicit custom escape byte is honored: the backslash stays data and
-- the escape byte protects a field separator
drop table if exists t31232_inline_esc_custom;
create table t31232_inline_esc_custom(id int primary key, s varchar(40));
load data inline format='csv', data='3,row\\8\n4,a|,b\n' into table t31232_inline_esc_custom fields terminated by ',' escaped by '|';
select * from t31232_inline_esc_custom order by id;
drop table t31232_inline_esc_custom;

-- exact-byte omitted-clause controls: byte-identical payloads show the
-- mode-specific omitted defaults. Inline keeps the backslash as plain data;
-- the file path consumes it as the default escape.
drop table if exists t31232_esc_omitted_inline;
create table t31232_esc_omitted_inline(id int primary key, s varchar(40));
load data inline format='csv', data='1,row\\8\n2,cept\\9z\n' into table t31232_esc_omitted_inline fields terminated by ',';
select * from t31232_esc_omitted_inline order by id;
drop table if exists t31232_esc_omitted_file;
create table t31232_esc_omitted_file(id int primary key, s varchar(40));
load data infile '$resources/load_data/issue_31232_esc_pair.csv' into table t31232_esc_omitted_file fields terminated by ',';
select * from t31232_esc_omitted_file order by id;
drop table t31232_esc_omitted_inline;
drop table t31232_esc_omitted_file;

-- literal-backslash parity between the modes is asserted with an explicit
-- empty ESCAPED BY: byte-identical payloads load identical rows.
drop table if exists t31232_esc_empty_inline;
create table t31232_esc_empty_inline(id int primary key, s varchar(40));
load data inline format='csv', data='1,row\\8\n2,cept\\9z\n' into table t31232_esc_empty_inline fields terminated by ',' escaped by '';
select * from t31232_esc_empty_inline order by id;
drop table if exists t31232_esc_empty_file;
create table t31232_esc_empty_file(id int primary key, s varchar(40));
load data infile '$resources/load_data/issue_31232_esc_pair.csv' into table t31232_esc_empty_file fields terminated by ',' escaped by '';
select * from t31232_esc_empty_file order by id;
drop table t31232_esc_empty_inline;
drop table t31232_esc_empty_file;
