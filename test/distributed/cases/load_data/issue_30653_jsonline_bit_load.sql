-- Issue #30653 (JSONLINE half): BIT columns exported to JSONLINE files are
-- canonical nonnegative integer JSON NUMBER tokens, and the real file
-- consumers must preserve the exact uint64. This public case drives the
-- directly decoded serial LOAD (all tiny fixtures sit far below the 128MB
-- parallel-admission gate LoadParallelMinSize=WriteS3Threshold, so the serial
-- reader compiles) plus the compressed extension variant; the varchar-staged
-- cast consumer is proven by the accepted native UT (NewCast/strToBit).
-- Byte semantics for JSON strings, JSON null, raw-field NULLIF matching
-- before any numeric conversion, and the strict out-of-width rejection stay
-- pinned. Exported-file byte identity (map marshaling key order, canonical
-- numbers) is checked by the owned independent JSON-line checker; loaded
-- values are re-read through SQL.

drop database if exists jsonline_bit_30653;
create database jsonline_bit_30653;
use jsonline_bit_30653;
create table nums (id int primary key, b1 bit(1), b8 bit(8), b64 bit(64));
load data infile {'filepath'='$resources/load_data/issue_30653_bit_object.jl','format'='jsonline','jsondata'='object'} into table nums;
select id, cast(b1 as unsigned) as b1v, cast(b8 as unsigned) as b8v, cast(b64 as unsigned) as b64v from nums order by id;
select id, b1, b8, b64 from nums order by id into outfile '$resources/into_outfile/issue_30653_bit_export.jsonl' format 'jsonline';
create table nums_rt (id int primary key, b1 bit(1), b8 bit(8), b64 bit(64));
load data infile {'filepath'='$resources/into_outfile/issue_30653_bit_export.jsonl','format'='jsonline','jsondata'='object'} into table nums_rt;
select id, cast(b1 as unsigned) as b1v, cast(b8 as unsigned) as b8v, cast(b64 as unsigned) as b64v from nums_rt order by id;
create table gz_nums (id int primary key, b1 bit(1), b8 bit(8), b64 bit(64));
load data infile {'filepath'='$resources/load_data/issue_30653_bit_object.jl.gz','format'='jsonline','jsondata'='object'} into table gz_nums;
select count(*) as n, max(cast(b8 as unsigned)) as mx8, max(cast(b64 as unsigned)) as mx64 from gz_nums;
create table arr8 (b8 bit(8));
load data infile {'filepath'='$resources/load_data/issue_30653_bit_array8.jl','format'='jsonline','jsondata'='array'} into table arr8;
select count(*) as n, max(cast(b8 as unsigned)) as mx, min(cast(b8 as unsigned)) as mn from arr8;
create table arr64 (b64 bit(64));
load data infile {'filepath'='$resources/load_data/issue_30653_bit_array64.jl','format'='jsonline','jsondata'='array'} into table arr64;
select count(*) as n, max(cast(b64 as unsigned)) as mx, min(cast(b64 as unsigned)) as mn from arr64;
create table nf_t (id int primary key, b64 bit(64));
load data infile {'filepath'='$resources/load_data/issue_30653_bit_nullif.jl','format'='jsonline','jsondata'='object'} into table nf_t set b64=nullif(b64,1);
select id, cast(b64 as unsigned) as b64v from nf_t order by id;
create table nfa_t (id int primary key, b64 bit(64));
load data infile {'filepath'='$resources/load_data/issue_30653_bit_nullif.jl','format'='jsonline','jsondata'='object'} into table nfa_t set b64=nullif(b64,'A');
select id, cast(b64 as unsigned) as b64v from nfa_t order by id;
create table err8 (id int primary key, b8 bit(8));
-- @regex("data too long, type width = 8", true)
load data infile {'filepath'='$resources/load_data/issue_30653_bit_err8.jl','format'='jsonline','jsondata'='object'} into table err8;
select count(*) as n from err8;
set save_query_result = on;
/* save_result */ select id, b8, b64 from nums where id in (2,7,8) order by id;
select id, b8, b64 from result_scan(last_query_id()) as r into outfile '$resources/into_outfile/issue_30653_bit_rs.jsonl' format 'jsonline';
set save_query_result = off;
create table rs_t (id int primary key, b8 bit(8), b64 bit(64));
load data infile {'filepath'='$resources/into_outfile/issue_30653_bit_rs.jsonl','format'='jsonline','jsondata'='object'} into table rs_t;
select id, cast(b8 as unsigned) as b8v, cast(b64 as unsigned) as b64v from rs_t order by id;
drop database if exists jsonline_bit_30653;
