-- CSV format options for writable external tables: each section round-trips
-- data through a table whose FIELDS/LINES configuration stresses one option
-- (or a combination), asserting the reader parses back exactly what was
-- inserted. Files are written flat into the stage with per-table prefixes.

drop database if exists wcsv;
create database wcsv;
use wcsv;

drop stage if exists cstage;
create stage cstage url = 'file:///$resources/into_outfile/stage';

-- Source rows stress every structural character at once: field separator
-- candidates, both quote kinds, backslash, the '!' escape candidate, an
-- empty string, a NULL, unicode, and spaces.
drop table if exists src;
create table src(a int, b varchar(60));
insert into src values
  (1, 'plain'),
  (2, 'comma,inside'),
  (3, 'double"quote'),
  (4, 'single''quote'),
  (5, 'back\\slash'),
  (6, 'bang!mark'),
  (7, 'pipe|char'),
  (8, ''),
  (9, null),
  (10, '中文, déjà vu'),
  (11, '  padded  ');

-- ---------- FIELDS TERMINATED BY: single custom char ----------
drop table if exists t_pipe;
create external table t_pipe(a int, b varchar(60))
infile{'filepath'='stage://cstage/copt_pipe_*.csv', 'format'='csv', 'write_file_pattern'='stage://cstage/copt_pipe_%U.csv'}
fields terminated by '|';
insert into t_pipe select * from src;
select a, concat('[', b, ']') from t_pipe order by a;
select count(*), count(b) from t_pipe;

-- ---------- FIELDS TERMINATED BY: tab ----------
drop table if exists t_tab;
create external table t_tab(a int, b varchar(60))
infile{'filepath'='stage://cstage/copt_tab_*.csv', 'format'='csv', 'write_file_pattern'='stage://cstage/copt_tab_%U.csv'}
fields terminated by '\t';
insert into t_tab select * from src;
select count(*), count(b), min(a), max(a) from t_tab;

-- ---------- FIELDS TERMINATED BY: multi-char ----------
drop table if exists t_multi;
create external table t_multi(a int, b varchar(60))
infile{'filepath'='stage://cstage/copt_multi_*.csv', 'format'='csv', 'write_file_pattern'='stage://cstage/copt_multi_%U.csv'}
fields terminated by '||';
insert into t_multi select * from src;
select a, concat('[', b, ']') from t_multi order by a;

-- ---------- ENCLOSED BY: single quote, data containing both quote kinds ----------
drop table if exists t_squote;
create external table t_squote(a int, b varchar(60))
infile{'filepath'='stage://cstage/copt_sq_*.csv', 'format'='csv', 'write_file_pattern'='stage://cstage/copt_sq_%U.csv'}
fields terminated by ',' enclosed by '\'';
insert into t_squote select * from src;
select a, concat('[', b, ']') from t_squote order by a;

-- ---------- ENCLOSED BY '': explicit empty falls back to '"' like the reader ----------
drop table if exists t_noql;
create external table t_noql(a int, b varchar(60))
infile{'filepath'='stage://cstage/copt_noq_*.csv', 'format'='csv', 'write_file_pattern'='stage://cstage/copt_noq_%U.csv'}
fields terminated by ',' enclosed by '';
insert into t_noql select * from src;
select a, concat('[', b, ']') from t_noql order by a;

-- ---------- ESCAPED BY: custom '!' with default '"' enclosure ----------
drop table if exists t_bang;
create external table t_bang(a int, b varchar(60))
infile{'filepath'='stage://cstage/copt_bang_*.csv', 'format'='csv', 'write_file_pattern'='stage://cstage/copt_bang_%U.csv'}
fields terminated by ',' escaped by '!';
insert into t_bang select * from src;
select a, concat('[', b, ']') from t_bang order by a;

-- ---------- ESCAPED BY '' (disabled) ----------
drop table if exists t_noesc;
create external table t_noesc(a int, b varchar(60))
infile{'filepath'='stage://cstage/copt_noesc_*.csv', 'format'='csv', 'write_file_pattern'='stage://cstage/copt_noesc_%U.csv'}
fields terminated by ',' escaped by '';
insert into t_noesc select * from src;
select a, concat('[', b, ']') from t_noesc order by a;

-- ---------- LINES TERMINATED BY: \r\n ----------
drop table if exists t_crlf;
create external table t_crlf(a int, b varchar(60))
infile{'filepath'='stage://cstage/copt_crlf_*.csv', 'format'='csv', 'write_file_pattern'='stage://cstage/copt_crlf_%U.csv'}
fields terminated by ',' lines terminated by '\r\n';
insert into t_crlf select * from src;
select a, concat('[', b, ']') from t_crlf order by a;

-- ---------- LINES TERMINATED BY: multi-char text terminator ----------
-- Values containing any terminator byte (E/O/L/#) are auto-enclosed.
drop table if exists t_eol;
create external table t_eol(a int, b varchar(60))
infile{'filepath'='stage://cstage/copt_eol_*.csv', 'format'='csv', 'write_file_pattern'='stage://cstage/copt_eol_%U.csv'}
fields terminated by ',' lines terminated by '#EOL#';
insert into t_eol select * from src;
select a, concat('[', b, ']') from t_eol order by a;

-- A value containing a real newline is data, not a record boundary, when the
-- line terminator is a custom string.
drop table if exists nl_src;
create table nl_src(a int, b varchar(60));
insert into nl_src values (1, 'first\nsecond'), (2, 'plain');
drop table if exists t_eolnl;
create external table t_eolnl(a int, b varchar(60))
infile{'filepath'='stage://cstage/copt_eolnl_*.csv', 'format'='csv', 'write_file_pattern'='stage://cstage/copt_eolnl_%U.csv'}
fields terminated by ',' lines terminated by '#EOL#';
insert into t_eolnl select * from nl_src;
select a, replace(b, '\n', '<NL>') as b from t_eolnl order by a;

-- ---------- LINES STARTING BY, including the prefix inside data ----------
drop table if exists sb_src;
create table sb_src(a int, b varchar(60));
insert into sb_src values (1, 'plain'), (2, 'contains row: inside'), (3, 'row:starts');
drop table if exists t_sb;
create external table t_sb(a int, b varchar(60))
infile{'filepath'='stage://cstage/copt_sb_*.csv', 'format'='csv', 'write_file_pattern'='stage://cstage/copt_sb_%U.csv'}
fields terminated by ',' lines starting by 'row:';
insert into t_sb select * from sb_src;
select a, concat('[', b, ']') from t_sb order by a;

-- ---------- everything at once ----------
-- custom separator + single-quote enclosure + '!' escape + STARTING BY +
-- custom line terminator, against the full tricky data set. (The separator is
-- '^' because mo-tester's statement splitter trips on a quoted semicolon
-- inside SQL text.)
drop table if exists t_all;
create external table t_all(a int, b varchar(60))
infile{'filepath'='stage://cstage/copt_all_*.csv', 'format'='csv', 'write_file_pattern'='stage://cstage/copt_all_%U.csv'}
fields terminated by '^' enclosed by '\'' escaped by '!' lines starting by 'R>' terminated by '#EOL#';
insert into t_all select * from src;
select a, concat('[', b, ']') from t_all order by a;
select count(*), count(b) from t_all;

-- ---------- typed values colliding with separators ----------
-- time with ':' separator, decimal with '.' separator: bare values that
-- contain the separator byte are auto-enclosed by the writer.
drop table if exists t_time;
create external table t_time(a int, t time, d datetime)
infile{'filepath'='stage://cstage/copt_time_*.csv', 'format'='csv', 'write_file_pattern'='stage://cstage/copt_time_%U.csv'}
fields terminated by ':';
insert into t_time values (1, '12:34:56', '2026-06-12 08:09:10'), (2, '00:00:01', '1999-01-02 03:04:05');
select * from t_time order by a;

drop table if exists t_dot;
create external table t_dot(a int, f double, d decimal(10,3))
infile{'filepath'='stage://cstage/copt_dot_*.csv', 'format'='csv', 'write_file_pattern'='stage://cstage/copt_dot_%U.csv'}
fields terminated by '.';
insert into t_dot values (1, 1.5, 123.456), (2, -2.25, -0.001), (3, 42, 7);
select * from t_dot order by a;

-- ---------- multi-char terminator boundary overlap ----------
-- A value whose suffix is a prefix of the terminator must be enclosed: with
-- TERMINATED BY '00', an unenclosed 10 would otherwise scan as 1 + '00'.
drop table if exists t_b00;
create external table t_b00(a int, b int)
infile{'filepath'='stage://cstage/copt_b00_*.csv', 'format'='csv', 'write_file_pattern'='stage://cstage/copt_b00_%U.csv'}
fields terminated by '00';
insert into t_b00 values (10, 5), (100, 200), (1, 2);
select * from t_b00 order by a;

-- ---------- enum labels are enclosed (a 'NULL' label is data, not NULL) ----------
drop table if exists t_enum;
create external table t_enum(a int, e enum('NULL','red','dark blue'))
infile{'filepath'='stage://cstage/copt_enum_*.csv', 'format'='csv', 'write_file_pattern'='stage://cstage/copt_enum_%U.csv'}
fields terminated by ',';
insert into t_enum values (1, 'NULL'), (2, 'red'), (3, 'dark blue'), (4, null);
select a, e, e is null from t_enum order by a;

-- ---------- a trailing CR round-trips via escape ----------
-- The reader strips one trailing CR from each record (even quoted), so the
-- writer escapes CR bytes as \r when escaping is enabled.
drop table if exists t_cr;
create external table t_cr(a int, b varchar(20))
infile{'filepath'='stage://cstage/copt_cr_*.csv', 'format'='csv', 'write_file_pattern'='stage://cstage/copt_cr_%U.csv'}
fields terminated by ',';
insert into t_cr values (1, 'abc\r'), (2, 'x\ry');
select a, replace(b, '\r', '<CR>') as b from t_cr order by a;

-- ---------- values no encoding can round-trip are rejected ----------
-- a string of exactly \N null-matches on read outside the default escape
insert into t_bang values (99, '\\N');
-- and always under jsonline
drop table if exists t_jln;
create external table t_jln(a int, b varchar(20))
infile{'filepath'='stage://cstage/copt_jln_*.jl', 'format'='jsonline', 'jsondata'='object', 'write_file_pattern'='stage://cstage/copt_jln_%U.jl'};
insert into t_jln values (1, '\\N');
drop table if exists t_jln;

-- ---------- NULL vs empty string under custom options ----------
drop table if exists t_null;
create external table t_null(a int, b varchar(60))
infile{'filepath'='stage://cstage/copt_null_*.csv', 'format'='csv', 'write_file_pattern'='stage://cstage/copt_null_%U.csv'}
fields terminated by '|' escaped by '!';
insert into t_null values (1, null), (2, ''), (3, 'x');
select a, b is null, length(b) from t_null order by a;

-- ---------- LOAD: the source clause describes the input file; the target
-- table's stored FIELDS describe the written output ----------
drop table if exists t_load;
create external table t_load(col1 date not null, col2 datetime, col3 timestamp, col4 bool)
infile{'filepath'='stage://cstage/copt_load_*.csv', 'format'='csv', 'write_file_pattern'='stage://cstage/copt_load_%U.csv'}
fields terminated by '|';
set time_zone = 'SYSTEM';
load data infile '$resources/load_data/time_date_1.csv' into table t_load fields terminated by ',';
select * from t_load order by col1;

-- ---------- SHOW CREATE preserves the FIELDS/LINES clauses ----------
show create table t_all;

drop table if exists t_pipe;
drop table if exists t_tab;
drop table if exists t_multi;
drop table if exists t_squote;
drop table if exists t_noql;
drop table if exists t_bang;
drop table if exists t_noesc;
drop table if exists t_crlf;
drop table if exists t_eol;
drop table if exists t_eolnl;
drop table if exists nl_src;
drop table if exists t_sb;
drop table if exists sb_src;
drop table if exists t_all;
drop table if exists t_time;
drop table if exists t_dot;
drop table if exists t_null;
drop table if exists t_b00;
drop table if exists t_enum;
drop table if exists t_cr;
drop table if exists t_load;
drop table if exists src;
drop stage if exists cstage;
drop database if exists wcsv;
