-- Coverage for FIELDS ESCAPED BY and LINES STARTING BY in SELECT INTO OUTFILE
-- (issue #30710 / consolidated #30754: grammar parity with LOAD DATA, and the
-- writer honoring both clauses byte-exactly).
drop database if exists outfile_fields_lines;
create database outfile_fields_lines;
use outfile_fields_lines;

drop stage if exists outfile_fields_lines_stage;
create stage outfile_fields_lines_stage URL='file://$resources/into_outfile/outfile_fields_lines';

create table src (id int, s varchar(20));
insert into src values
(1,'alpha'),
(2,'be,ns'),
(3,'has"quote'),
(4,'#hash'),
(5,null);

-- The exact statements from #30754: previously a parse error, now exported.
select 1 into outfile 'stage://outfile_fields_lines_stage/o1.csv'
fields terminated by ',' enclosed by '' escaped by '\\' header 'false';
select 1 into outfile 'stage://outfile_fields_lines_stage/o2.csv'
lines starting by '>>' terminated by '\n' header 'false';
select load_file(cast('stage://outfile_fields_lines_stage/o1.csv' as datalink));
select load_file(cast('stage://outfile_fields_lines_stage/o2.csv' as datalink));

-- LINES STARTING BY prefixes every data record; the header line is NOT
-- prefixed, so LOAD skips it instead of importing it as a data row.
select id, s from src order by id
into outfile 'stage://outfile_fields_lines_stage/prefix.csv'
lines starting by '>>' terminated by '\n';
select load_file(cast('stage://outfile_fields_lines_stage/prefix.csv' as datalink));
create table back1 (id int, s varchar(20));
load data infile 'stage://outfile_fields_lines_stage/prefix.csv' into table back1
fields terminated by ',' enclosed by '"' lines starting by '>>' terminated by '\n';
select * from back1 order by id;

-- A custom FIELDS ESCAPED BY byte protects itself and the separators on write
-- and must round-trip through the matching LOAD clause.
select id, s from src order by id
into outfile 'stage://outfile_fields_lines_stage/esc.csv'
fields terminated by ',' enclosed by '' escaped by '#' header 'false';
select load_file(cast('stage://outfile_fields_lines_stage/esc.csv' as datalink));
create table back2 (id int, s varchar(20));
load data infile 'stage://outfile_fields_lines_stage/esc.csv' into table back2
fields terminated by ',' enclosed by '' escaped by '#' lines terminated by '\n';
select * from back2 order by id;

-- ESCAPED BY '' disables escaping: values are written verbatim. The embedded
-- separator can then no longer round-trip (documented MySQL semantics), so
-- only the write result is pinned here.
select id, s from src where id = 2
into outfile 'stage://outfile_fields_lines_stage/verbatim.csv'
fields terminated by ',' enclosed by '' escaped by '' header 'false';
select load_file(cast('stage://outfile_fields_lines_stage/verbatim.csv' as datalink));

-- Default framing (no clauses) stays byte-identical.
select id, s from src order by id
into outfile 'stage://outfile_fields_lines_stage/defaults.csv' header 'false';
select load_file(cast('stage://outfile_fields_lines_stage/defaults.csv' as datalink));

-- Header/prefix collision (review probe): LOAD's startingBy filter skips a
-- line only when the prefix is absent from it. A column named `name` with
-- LINES STARTING BY 'n' would load a mangled "ame" header row, so the export
-- is rejected before any file is opened. HEADER 'false' is the escape hatch
-- and round-trips with no ghost row.
select 'alpha' as name into outfile 'stage://outfile_fields_lines_stage/collide.csv'
lines starting by 'n';
select 1 as id, 'alpha' as data into outfile 'stage://outfile_fields_lines_stage/collide2.csv'
lines starting by 'd,d';
select 'alpha' as name into outfile 'stage://outfile_fields_lines_stage/tagged.csv'
lines starting by 'n' header 'false';
select load_file(cast('stage://outfile_fields_lines_stage/tagged.csv' as datalink));
create table hdr_back (s varchar(10));
load data infile 'stage://outfile_fields_lines_stage/tagged.csv' into table hdr_back
fields terminated by ',' lines starting by 'n' terminated by '\n' (s);
select * from hdr_back;
drop table hdr_back;

-- A literal '\N' value is LOAD's NULL token even inside quotes
-- (quotedNullIsText=false). With a non-default FIELDS ESCAPED BY the written
-- bytes would silently reload as SQL NULL, so the export is rejected up front
-- and no file may be created. Genuine NULLs are unaffected.
create table rt (id int, s varchar(20));
insert into rt values (1,'\\N'),(2,null),(3,'plain');
select id, s from rt order by id
into outfile 'stage://outfile_fields_lines_stage/nt_hash.csv'
fields terminated by ',' enclosed by '' escaped by '#' header 'false';
select count(*) from stage_list('stage://outfile_fields_lines_stage/nt_hash.csv');
select id, s from rt where id = 1
into outfile 'stage://outfile_fields_lines_stage/nt_empty.csv'
fields terminated by ',' enclosed by '' escaped by '' header 'false';
select count(*) from stage_list('stage://outfile_fields_lines_stage/nt_empty.csv');

-- The default escape rewrites the literal to \\N, so it round-trips as text
-- while a real NULL keeps the bare \N marker.
select id, s from rt order by id
into outfile 'stage://outfile_fields_lines_stage/nt_default.csv'
fields terminated by ',' enclosed by '' header 'false';
select load_file(cast('stage://outfile_fields_lines_stage/nt_default.csv' as datalink));
create table rt_back (id int, s varchar(20));
load data infile 'stage://outfile_fields_lines_stage/nt_default.csv' into table rt_back
fields terminated by ',' enclosed by '' lines terminated by '\n';
select id, s, s is null as is_null from rt_back order by id;

-- NULLs also export under a custom escape: the NULL marker is not a value.
select id, s from rt where id in (2,3) order by id
into outfile 'stage://outfile_fields_lines_stage/nt_nulls.csv'
fields terminated by ',' enclosed by '' escaped by '#' header 'false';
select load_file(cast('stage://outfile_fields_lines_stage/nt_nulls.csv' as datalink));
create table rt_nulls (id int, s varchar(20));
load data infile 'stage://outfile_fields_lines_stage/nt_nulls.csv' into table rt_nulls
fields terminated by ',' enclosed by '' escaped by '#' lines terminated by '\n';
select id, s is null as is_null from rt_nulls order by id;

-- With FIELDS ESCAPED BY '' the reader strips a trailing CR of the last
-- column's record under '\n' termination. The strip runs after unquoting, so
-- an enclosure does not protect it either: both exports are rejected. Any
-- escape byte rewrites the CR and round-trips.
create table rtcr (id int, s varchar(20));
insert into rtcr values (1,'a\r');
select id, s from rtcr
into outfile 'stage://outfile_fields_lines_stage/cr_bare.csv'
fields terminated by ',' enclosed by '' escaped by '' header 'false';
select count(*) from stage_list('stage://outfile_fields_lines_stage/cr_bare.csv');
select id, s from rtcr
into outfile 'stage://outfile_fields_lines_stage/cr_quoted.csv'
fields terminated by ',' enclosed by '"' escaped by '' header 'false';
select count(*) from stage_list('stage://outfile_fields_lines_stage/cr_quoted.csv');
-- CR in a non-last column is followed by the separator and keeps its byte
-- (file content pinned via hex: a raw CR would confuse line-based readers):
select s, id from rtcr
into outfile 'stage://outfile_fields_lines_stage/cr_first.csv'
fields terminated by ',' enclosed by '' escaped by '' header 'false';
select hex(load_file(cast('stage://outfile_fields_lines_stage/cr_first.csv' as datalink)));
create table rtcr_first (s varchar(20), id int);
load data infile 'stage://outfile_fields_lines_stage/cr_first.csv' into table rtcr_first
fields terminated by ',' enclosed by '' escaped by '' lines terminated by '\n';
select id, hex(s) as hs from rtcr_first;
-- A custom escape protects the CR in the last column:
select id, s from rtcr
into outfile 'stage://outfile_fields_lines_stage/cr_hash.csv'
fields terminated by ',' enclosed by '' escaped by '#' header 'false';
select load_file(cast('stage://outfile_fields_lines_stage/cr_hash.csv' as datalink));
create table rtcr_hash (id int, s varchar(20));
load data infile 'stage://outfile_fields_lines_stage/cr_hash.csv' into table rtcr_hash
fields terminated by ',' enclosed by '' escaped by '#' lines terminated by '\n';
select id, hex(s) as hs from rtcr_hash;
drop table rtcr_hash;
drop table rtcr_first;
drop table rtcr;
drop table rt_nulls;
drop table rt_back;
drop table rt;

remove files from stage if exists 'stage://outfile_fields_lines_stage/*.csv';
select count(*) from stage_list('stage://outfile_fields_lines_stage/');
drop stage outfile_fields_lines_stage;
drop table back2;
drop table back1;
drop table src;
drop database outfile_fields_lines;
