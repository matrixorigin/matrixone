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

remove files from stage if exists 'stage://outfile_fields_lines_stage/*.csv';
select count(*) from stage_list('stage://outfile_fields_lines_stage/');
drop stage outfile_fields_lines_stage;
drop table back2;
drop table back1;
drop table src;
drop database outfile_fields_lines;
