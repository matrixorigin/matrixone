-- Issue #31277: a JSON column can hold both a top-level JSON literal null
-- and a SQL NULL, but a JSONLINE line encodes both as the same bare null,
-- which the jsonline loader always reads back as SQL NULL. JSONLINE OUTFILE
-- must reject the unrepresentable top-level JSON null loudly instead of
-- silently changing the value, while SQL NULL, nested JSON null, the JSON
-- string "null" and ordinary JSON keep round-tripping exactly in both reader
-- modes. CSV OUTFILE/LOAD stays the lossless path for the full value space.

drop database if exists issue_31277;
create database issue_31277;
use issue_31277;

create table src (id int primary key, j json);
insert into src values (1, 'null'), (2, null), (3, '{"a":null}'), (4, '"null"'), (5, '{"a":1}');
-- Five distinct states: JSON literal null, SQL NULL, nested JSON null, the
-- JSON string "null" and ordinary JSON.
select id, j, j is null from src order by id;

-- The full export is rejected: no JSONLINE line can carry id=1 faithfully.
-- @regex("JSONLINE OUTFILE cannot encode a top-level JSON null", true)
select id, j from src order by id into outfile '$resources/into_outfile/issue_31277.jsonl' format 'jsonline';

-- Every representable row exports with an exact, loadable encoding.
select id, j from src where id <> 1 order by id into outfile '$resources/into_outfile/issue_31277.jsonl' format 'jsonline';
select load_file(cast('file://$resources/into_outfile/issue_31277.jsonl' as datalink)) as jsonl_content;

-- Object mode loads every line back with the source semantics.
create table object_dst (id int primary key, j json);
load data infile {'filepath'='$resources/into_outfile/issue_31277.jsonl','format'='jsonline','jsondata'='object'} into table object_dst;
select id, j, j is null from object_dst order by id;

-- The array-mode reader keeps the same bare-null -> SQL NULL contract.
create table array_dst (id int primary key, j json);
load data infile {'filepath'='$resources/load_data/issue_31277_jsonline_json_null_array.jl','format'='jsonline','jsondata'='array'} into table array_dst;
select id, j, j is null from array_dst order by id;

-- A bare null into a NOT NULL JSON column is rejected and leaves no rows.
create table not_null_dst (j json not null);
-- @regex("cannot be null", true)
load data infile {'filepath'='$resources/load_data/issue_31277_jsonline_json_null_nullonly.jl','format'='jsonline','jsondata'='object'} into table not_null_dst;
select count(*) as rows_after_rejected_null from not_null_dst;

-- CSV is the lossless path for this issue's value space: the top-level JSON
-- null and the SQL NULL stay distinct tokens and objects round-trip exactly.
-- Top-level JSON string scalars are excluded: the pre-existing CSV JSON
-- loader trims surrounding quotes from every field, a separate contract
-- outside this issue.
select id, j from src where id <> 4 order by id into outfile '$resources/into_outfile/issue_31277.csv' fields terminated by ',' optionally enclosed by '"' header 'false';
select load_file(cast('file://$resources/into_outfile/issue_31277.csv' as datalink)) as csv_content;
create table csv_dst (id int primary key, j json);
load data infile '$resources/into_outfile/issue_31277.csv' into table csv_dst fields terminated by ',' optionally enclosed by '"';
select id, j, j is null from csv_dst order by id;

drop database issue_31277;
show databases like 'issue_31277';
