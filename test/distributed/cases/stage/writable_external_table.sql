-- Writable external tables: INSERT ... SELECT and LOAD into an external table
-- whose WRITE_FILE_PATTERN points to a stage. Each insert pipeline writes a new
-- file; reading back goes through the external table's read glob. Output files
-- are written flat into the stage dir with a per-table prefix so the read glob
-- only matches this test's files.

drop database if exists wext;
create database wext;
use wext;

drop stage if exists wstage;
create stage wstage url = 'file:///$resources/into_outfile/stage';

drop table if exists src;
create table src(a int, b varchar(20), c double);
insert into src values (1,'alice',1.5),(2,'bob',2.5),(3,'carol',3.5);

-- ---------- CSV writable external table ----------
drop table if exists ext_csv;
create external table ext_csv(a int, b varchar(20), c double)
infile{'filepath'='stage://wstage/wext_csv_*.csv', 'format'='csv', 'write_file_pattern'='stage://wstage/wext_csv_%U.csv'}
fields terminated by ',';

-- SHOW CREATE TABLE keeps WRITE_FILE_PATTERN, so the table can be recreated
-- as a writable external table from its own output.
show create table ext_csv;

-- INSERT ... SELECT into the external table, then read it back.
insert into ext_csv select * from src;
select * from ext_csv order by a;
select count(*) from ext_csv;

-- A second insert writes a new file; the read glob now sees both files.
insert into ext_csv select a+10, b, c from src;
select * from ext_csv order by a;
select count(*) from ext_csv;

-- ---------- CSV default-enclosure round-trip ----------
-- No ENCLOSED BY on the table: the writer quotes strings with the reader's
-- default '"' so values containing the field terminator, quotes, newlines or
-- backslashes still round-trip.
drop table if exists tricky_src;
create table tricky_src(a int, b varchar(50));
insert into tricky_src values (1,'with,comma'),(2,'with"quote'),(3,'with\nnewline'),(4,'with\\backslash');
drop table if exists ext_tricky;
create external table ext_tricky(a int, b varchar(50))
infile{'filepath'='stage://wstage/wext_tricky_*.csv', 'format'='csv', 'write_file_pattern'='stage://wstage/wext_tricky_%U.csv'}
fields terminated by ',';
insert into ext_tricky select * from tricky_src;
select a, replace(b, '\n', '<NL>') as b from ext_tricky order by a;

-- ---------- parallel (multi-pipeline) insert ----------
-- Enough rows that the insert runs on several parallel pipelines (each owning
-- one writer/file); duplicated operator instances must stay in external-write
-- mode instead of degrading to engine-relation inserts.
drop table if exists big_src;
create table big_src(a int, b varchar(30));
insert into big_src select result, concat('row-', result) from generate_series(1, 100000) g;
drop table if exists ext_big;
create external table ext_big(a int, b varchar(30))
infile{'filepath'='stage://wstage/wext_big_*.csv', 'format'='csv', 'write_file_pattern'='stage://wstage/wext_big_%U.csv'}
fields terminated by ',';
insert into ext_big select * from big_src;
select count(*), min(a), max(a) from ext_big;

-- ---------- remote run (multi-CN dispatch) ----------
-- A flushed source above the multi-CN stats thresholds (>512 blocks, like the
-- optimizer/shuffle cases) compiles the INSERT to a MULTICN plan: on a
-- multi-CN cluster, source-scan scopes — with the external-write insert on
-- top — are dispatched to remote CNs through the pipeline protocol,
-- exercising the to_external encode/decode and the remote writer rebuild; on
-- a single CN it degenerates to the parallel case. Results are identical
-- either way.
drop table if exists remote_src;
create table remote_src(a int, b varchar(30));
insert into remote_src select result, concat('r-', result) from generate_series(1, 4400000) g;
-- @separator:table
select mo_ctl('dn', 'flush', 'wext.remote_src');
select sleep(1);
drop table if exists ext_remote;
create external table ext_remote(a int, b varchar(30))
infile{'filepath'='stage://wstage/wext_remote_*.csv', 'format'='csv', 'write_file_pattern'='stage://wstage/wext_remote_%U.csv'}
fields terminated by ',';
insert into ext_remote select * from remote_src;
select count(*), min(a), max(a) from ext_remote;

-- ---------- JSONLine writable external table ----------
drop table if exists ext_jl;
create external table ext_jl(a int, b varchar(20), c double)
infile{'filepath'='stage://wstage/wext_jl_*.jl', 'format'='jsonline', 'write_file_pattern'='stage://wstage/wext_jl_%U.jl', 'jsondata'='object'}
fields terminated by ',';
insert into ext_jl select * from src;
select * from ext_jl order by a;

-- ---------- wide column-type coverage (CSV + JSONLine), incl. NULLs ----------
drop table if exists wide_src;
create table wide_src(
  c_i8 tinyint, c_i64 bigint, c_u32 int unsigned,
  c_f32 float, c_dec decimal(10,2),
  c_ch char(4), c_vc varchar(20), c_txt text,
  c_dt date, c_bool bool, c_bit bit(8), c_json json);
insert into wide_src values
  (-1, 9223372036854775807, 4000000000, 1.5, 123.45,
   'ab', 'hi,there', 'long text', '2026-06-08', true, b'101', '{"k":1}'),
  (null, null, null, null, null, null, null, null, null, null, null, null);

drop table if exists ext_wide_csv;
create external table ext_wide_csv(
  c_i8 tinyint, c_i64 bigint, c_u32 int unsigned,
  c_f32 float, c_dec decimal(10,2),
  c_ch char(4), c_vc varchar(20), c_txt text,
  c_dt date, c_bool bool, c_bit bit(8), c_json json)
infile{'filepath'='stage://wstage/wext_wide_*.csv', 'format'='csv', 'write_file_pattern'='stage://wstage/wext_wide_%U.csv'}
fields terminated by ',' enclosed by '"';
insert into ext_wide_csv select * from wide_src;
select c_i8, c_i64, c_u32, c_dec, c_vc, c_bool from ext_wide_csv order by c_i64;

-- jsonline writable tables reject bit columns (raw bytes cannot round-trip
-- through JSON strings), so the jsonline wide table omits c_bit.
drop table if exists ext_wide_jl;
create external table ext_wide_jl(
  c_i8 tinyint, c_i64 bigint, c_u32 int unsigned,
  c_f32 float, c_dec decimal(10,2),
  c_ch char(4), c_vc varchar(20), c_txt text,
  c_dt date, c_bool bool, c_json json)
infile{'filepath'='stage://wstage/wext_widejl_*.jl', 'format'='jsonline', 'write_file_pattern'='stage://wstage/wext_widejl_%U.jl', 'jsondata'='object'}
fields terminated by ',';
insert into ext_wide_jl select c_i8, c_i64, c_u32, c_f32, c_dec, c_ch, c_vc, c_txt, c_dt, c_bool, c_json from wide_src;
-- jsonline-object reads map fields by name, so validate the full round-trip with
-- "select *" (a projected column subset hits an unrelated pre-existing limitation
-- in the jsonline-object reader).
select * from ext_wide_jl order by c_i64;

-- ---------- bit values with tricky bytes round-trip through CSV ----------
-- bit bytes can collide with the field terminator or quote; the writer
-- encloses and escapes them like binary values. 44=',' 34='"' 92='\' 128=high
-- byte. (Bytes that are pure whitespace, e.g. 10='\n', are written correctly
-- but read back as NULL: the external reader TrimSpaces non-string fields — a
-- pre-existing read-side limitation, not specific to writable tables.)
drop table if exists bit_src;
create table bit_src(a int, b bit(8));
insert into bit_src values (1, 44), (2, 92), (3, 34), (4, 128), (5, 5);
drop table if exists ext_bit;
create external table ext_bit(a int, b bit(8))
infile{'filepath'='stage://wstage/wext_bit_*.csv', 'format'='csv', 'write_file_pattern'='stage://wstage/wext_bit_%U.csv'}
fields terminated by ',';
insert into ext_bit select * from bit_src;
select a, cast(b as unsigned) from ext_bit order by a;

-- ---------- NOT NULL is enforced ----------
drop table if exists ext_nn;
create external table ext_nn(a int not null, b varchar(10))
infile{'filepath'='stage://wstage/wext_nn_*.csv', 'format'='csv', 'write_file_pattern'='stage://wstage/wext_nn_%U.csv'}
fields terminated by ',';
insert into ext_nn values (1, 'ok');
insert into ext_nn values (null, 'boom');
insert into ext_nn select null, 'boom2';
select * from ext_nn order by a;

-- ---------- LOAD into a writable external table ----------
drop table if exists ext_load;
create external table ext_load(col1 date not null, col2 datetime, col3 timestamp, col4 bool)
infile{'filepath'='stage://wstage/wext_load_*.csv', 'format'='csv', 'write_file_pattern'='stage://wstage/wext_load_%U.csv'}
fields terminated by ',';
set time_zone = 'SYSTEM';
load data infile '$resources/load_data/time_date_1.csv' into table ext_load fields terminated by ',';
select * from ext_load order by col1;

-- ---------- error cases ----------
-- read-only external table (no WRITE_FILE_PATTERN) rejects writes
drop table if exists ext_ro;
create external table ext_ro(a int) infile 'stage://wstage/nonexist_*.csv';
insert into ext_ro values (1);

-- WRITE_FILE_PATTERN must resolve to a stage:// path
create external table ext_bad1(a int)
infile{'filepath'='stage://wstage/x_*.csv', 'format'='csv', 'write_file_pattern'='/tmp/part-%U.csv'};

-- only csv / jsonline are writable
create external table ext_bad2(a int)
infile{'filepath'='stage://wstage/x_*.pq', 'format'='parquet', 'write_file_pattern'='stage://wstage/x_%U.pq'};

-- bad strftime directive in the pattern
create external table ext_bad3(a int)
infile{'filepath'='stage://wstage/x_*.csv', 'format'='csv', 'write_file_pattern'='stage://wstage/x_%Q.csv'};

-- jsonline writable tables must use jsondata 'object' (the writer emits one
-- object per line, which an 'array' table could not read back)
create external table ext_bad4(a int)
infile{'filepath'='stage://wstage/x_*.jl', 'format'='jsonline', 'jsondata'='array', 'write_file_pattern'='stage://wstage/x_%U.jl'};

-- the pattern must contain %U or %nN: without one, parallel writers would all
-- expand to the same path and clobber each other
create external table ext_bad5(a int)
infile{'filepath'='stage://wstage/x_*.csv', 'format'='csv', 'write_file_pattern'='stage://wstage/out-%Y%m%d.csv'};

-- AUTO_INCREMENT needs the PreInsert operator, which the external plan skips
create external table ext_bad6(id int auto_increment, v int)
infile{'filepath'='stage://wstage/x_*.csv', 'format'='csv', 'write_file_pattern'='stage://wstage/x_%U.csv'};

-- bit columns cannot round-trip through JSON strings
create external table ext_bad7(b bit(8))
infile{'filepath'='stage://wstage/x_*.jl', 'format'='jsonline', 'jsondata'='object', 'write_file_pattern'='stage://wstage/x_%U.jl'};

-- REPLACE has no external-table support and reports the user-facing error
drop table if exists ext_rep;
create external table ext_rep(a int)
infile{'filepath'='stage://wstage/wext_rep_*.csv', 'format'='csv', 'write_file_pattern'='stage://wstage/wext_rep_%U.csv'};
replace into ext_rep values (1);
drop table if exists ext_rep;

drop table if exists ext_csv;
drop table if exists ext_jl;
drop table if exists ext_tricky;
drop table if exists tricky_src;
drop table if exists ext_big;
drop table if exists big_src;
drop table if exists ext_remote;
drop table if exists remote_src;
drop table if exists ext_bit;
drop table if exists bit_src;
drop table if exists ext_nn;
drop table if exists ext_wide_csv;
drop table if exists ext_wide_jl;
drop table if exists wide_src;
drop table if exists ext_load;
drop table if exists src;
drop stage if exists wstage;
drop database if exists wext;
