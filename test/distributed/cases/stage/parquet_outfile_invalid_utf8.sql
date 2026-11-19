-- Coverage for Parquet OUTFILE rejecting non-UTF-8 VARCHAR values (issue
-- #30710): a Parquet String() column is UTF-8 by definition, and MatrixOne's
-- own parquet loader refuses such bytes for VARCHAR targets, so the export
-- must fail up front instead of reporting success for an unloadable file.
-- Controls (valid UTF-8, VARBINARY/BINARY) must keep round-tripping lossless.
drop database if exists parquet_invalid_utf8;
create database parquet_invalid_utf8;
use parquet_invalid_utf8;

drop stage if exists parquet_invalid_utf8_stage;
create stage parquet_invalid_utf8_stage URL='file://$resources/into_outfile/parquet_invalid_utf8';

create table bad_src (id int primary key, s varchar(30));
insert into bad_src values (1,'plain'),(2,x'FF'),(3,'after'),(4,x'61FF62');

-- Issue #30710 reproduce: this statement used to succeed and write a file
-- LOAD DATA would then refuse. Now it fails, naming the same error the
-- loader reports, and leaves no partial file behind.
select id, s from bad_src order by id
into outfile 'stage://parquet_invalid_utf8_stage/bad.parquet' format 'parquet';
select count(*) from stage_list('stage://parquet_invalid_utf8_stage/');

-- The table is untouched by the failed export.
select id, hex(s) from bad_src order by id;

-- Control 1: valid UTF-8 VARCHAR (including NULL) round-trips exactly.
create table okv (id int primary key, s varchar(30));
insert into okv values (1,'héllo'),(2,'世界'),(3,null),(4,'plain');
select id, s from okv order by id
into outfile 'stage://parquet_invalid_utf8_stage/okv.parquet' format 'parquet';
create table okv_r like okv;
load data infile {'filepath'='stage://parquet_invalid_utf8_stage/okv.parquet','format'='parquet'} into table okv_r;
select id, hex(s) from okv_r order by id;

-- Control 2: the same non-UTF-8 bytes as VARBINARY stay lossless.
create table vb (id int primary key, s varbinary(30));
insert into vb values (1,x'706C61696E'),(2,x'FF'),(3,null),(4,x'61FF62');
select id, s from vb order by id
into outfile 'stage://parquet_invalid_utf8_stage/vb.parquet' format 'parquet';
create table vb_r like vb;
load data infile {'filepath'='stage://parquet_invalid_utf8_stage/vb.parquet','format'='parquet'} into table vb_r;
select id, hex(s) from vb_r order by id;

-- Control 3: BINARY (fixed width, NUL-padded) stays lossless.
create table bn (id int primary key, s binary(2));
insert into bn values (1,x'61FF'),(2,x'6162');
select id, s from bn order by id
into outfile 'stage://parquet_invalid_utf8_stage/bn.parquet' format 'parquet';
create table bn_r like bn;
load data infile {'filepath'='stage://parquet_invalid_utf8_stage/bn.parquet','format'='parquet'} into table bn_r;
select id, hex(s) from bn_r order by id;

-- Control 4: TEXT rejects invalid UTF-8 the same way (same String() node).
create table bad_text (id int primary key, s text);
insert into bad_text values (1,x'FF');
select id, s from bad_text order by id
into outfile 'stage://parquet_invalid_utf8_stage/bad_text.parquet' format 'parquet';
select count(*) from stage_list('stage://parquet_invalid_utf8_stage/') as remaining_files;

remove files from stage if exists 'stage://parquet_invalid_utf8_stage/*.parquet';
select count(*) from stage_list('stage://parquet_invalid_utf8_stage/');
drop stage parquet_invalid_utf8_stage;
drop table bad_text;
drop table bn_r;
drop table bn;
drop table vb_r;
drop table vb;
drop table okv_r;
drop table okv;
drop table bad_src;
drop database parquet_invalid_utf8;
