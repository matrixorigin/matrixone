-- Narrow vector (vecbf16 / vecf16 / vecint8 / vecuint8) SELECT ... INTO OUTFILE export,
-- covering BOTH batch routes: CSV (constructByte) and JSONL (constructJSONLine /
-- vectorValueToJSON). Regression: these batch routes previously errored
-- "constructByte/vectorValueToJSON: unsupported type" for narrow vectors. The JSONL route
-- must additionally render vecuint8 as a JSON number array, NOT base64 of the raw []byte,
-- and bf16/f16 as their float values (not the raw uint16 bit patterns).

drop database if exists nvec_export;
create database nvec_export;
use nvec_export;

create table t(id int primary key, bf vecbf16(3), h vecf16(3), i8 vecint8(3), u8 vecuint8(3));
insert into t values (1, '[1.5, 2.5, 3.5]', '[0.25, -1.75, 4]', '[1, -2, 3]', '[1, 2, 255]');
insert into t values (2, '[10, 20, 30]', '[0.5, 0.5, 0.5]', '[-128, 0, 127]', '[0, 128, 254]');
select * from t order by id;

-- ===== CSV batch export (constructByte) =====
select * from t order by id into outfile '$resources/into_outfile/narrow_vec.csv';
-- raw exported CSV: every narrow vector column is an array string (uint8 not raw bytes).
select load_file(cast('file://$resources/into_outfile/narrow_vec.csv' as datalink)) as csv_content;
-- the CSV round-trips back into identical narrow-vec columns.
create table t_csv(id int primary key, bf vecbf16(3), h vecf16(3), i8 vecint8(3), u8 vecuint8(3));
load data infile '$resources/into_outfile/narrow_vec.csv' into table t_csv fields terminated by ',' enclosed by '"' ignore 1 lines;
select * from t_csv order by id;

-- ===== JSONL batch export (constructJSONLine / vectorValueToJSON) =====
select * from t order by id into outfile '$resources/into_outfile/narrow_vec.jsonl';
-- raw exported JSONL: number arrays; vecuint8 is [1,2,255] (NOT a base64 string), and
-- bf16/f16 carry float values.
select load_file(cast('file://$resources/into_outfile/narrow_vec.jsonl' as datalink)) as jsonl_content;

drop database nvec_export;
