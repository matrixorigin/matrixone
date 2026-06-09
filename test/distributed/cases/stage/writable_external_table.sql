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

-- INSERT ... SELECT into the external table, then read it back.
insert into ext_csv select * from src;
select * from ext_csv order by a;
select count(*) from ext_csv;

-- A second insert writes a new file; the read glob now sees both files.
insert into ext_csv select a+10, b, c from src;
select * from ext_csv order by a;
select count(*) from ext_csv;

-- ---------- JSONLine writable external table ----------
drop table if exists ext_jl;
create external table ext_jl(a int, b varchar(20), c double)
infile{'filepath'='stage://wstage/wext_jl_*.jl', 'format'='jsonline', 'write_file_pattern'='stage://wstage/wext_jl_%U.jl', 'jsondata'='object'}
fields terminated by ',';
insert into ext_jl select * from src;
select * from ext_jl order by a;

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

drop table if exists ext_csv;
drop table if exists ext_jl;
drop table if exists ext_load;
drop table if exists src;
drop stage if exists wstage;
drop database if exists wext;
