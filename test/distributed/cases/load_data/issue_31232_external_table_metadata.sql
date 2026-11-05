-- #31232 pre-PR review follow-up: CREATE EXTERNAL TABLE publishes its parsed
-- source parameter into catalog metadata as the CREATE statement property.
-- A SET clause whose NULLIF wraps a row variable keeps that wrapper attached
-- to the statement (the LOAD replan path must rebind it), while a statement
-- expression cannot survive the metadata round-trip through encoding/json.
-- The property is therefore serialized from a stripped copy: the published
-- definition still decodes, SHOW CREATE TABLE renders the table, and scans
-- read the file. The column-form control keeps its reader options and its
-- sentinel map live, and both definitions drop cleanly with no residue.

drop database if exists external_31232_meta;
create database external_31232_meta;
use external_31232_meta;

-- Control: the column NULLIF form's metadata round-trips and the sentinel
-- mapping stays live (0 reads back as NULL, 7 stays 7).
drop table if exists e31232_ctrl;
create external table e31232_ctrl(id int, s varchar(20), flag int) infile{'filepath'='$resources/load_data/issue_31232_inline_set.csv','format'='csv'} fields terminated by ',' set flag=nullif(flag,0);
-- @regex("CREATE EXTERNAL TABLE `e31232_ctrl`", true)
-- @regex("`flag` int DEFAULT NULL", true)
-- @regex("FILEPATH'='[^']*load_data/issue_31232_inline_set[.]csv'", true)
-- @regex("'FORMAT'='csv'", true)
-- @regex("FIELDS TERMINATED BY ',' ENCLOSED BY ''", true)
-- @regex("INFILE[{]'FILEPATH'=''", false)
show create table e31232_ctrl;
select id, s, ifnull(flag,'N') as flag_isnull from e31232_ctrl order by id;
drop table if exists e31232_ctrl;

-- Row-variable form: accepted exactly as at base; the published definition
-- stays decodable and the table is fully usable. The form binds nothing at
-- scan time (an external table has no load column list), so the raw field
-- values read back unmodified.
drop table if exists e31232_rowvar;
create external table e31232_rowvar(id int, s varchar(20), flag int) infile{'filepath'='$resources/load_data/issue_31232_inline_set.csv','format'='csv'} fields terminated by ',' set flag=nullif(@v,'sentinel');
-- @regex("CREATE EXTERNAL TABLE `e31232_rowvar`", true)
-- @regex("`flag` int DEFAULT NULL", true)
-- @regex("FILEPATH'='[^']*load_data/issue_31232_inline_set[.]csv'", true)
-- @regex("'FORMAT'='csv'", true)
-- @regex("FIELDS TERMINATED BY ',' ENCLOSED BY ''", true)
-- @regex("INFILE[{]'FILEPATH'=''", false)
show create table e31232_rowvar;
select id, s, flag from e31232_rowvar order by id;
drop table if exists e31232_rowvar;

-- No residue: both definitions dropped cleanly and the session stays usable.
select count(*) as residue from information_schema.tables where table_schema='external_31232_meta' and table_name like 'e31232%';
drop database external_31232_meta;
select 'metadata_probe_done' as done_marker;
