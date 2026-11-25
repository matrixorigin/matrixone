-- Issue #32453: EXPLAIN ANALYZE LOAD DATA LOCAL INFILE crashed reading a nil
-- LOCAL pipe reader: executable explain wrappers run the wrapped statement in
-- the engine, but only a plain LOAD statement owns the client LOCAL upload
-- pipe. The unsupported combination is now rejected before compilation, before
-- any result metadata or LOCAL file request. The connection stays usable and
-- an ordinary LOCAL import on the same connection still works.
drop database if exists issue_32453_local_explain;
create database issue_32453_local_explain;
use issue_32453_local_explain;
create table t(id int, s varchar(10));
-- @regex("EXPLAIN ANALYZE LOAD DATA LOCAL INFILE is not supported", true)
explain analyze load data local infile {'filepath'='$resources/load_data/issue_32453_local_explain.jsonl','format'='jsonline','jsondata'='object'} into table t;
-- @regex("EXPLAIN PHYPLAN LOAD DATA LOCAL INFILE is not supported", true)
explain phyplan load data local infile {'filepath'='$resources/load_data/issue_32453_local_explain.jsonl','format'='jsonline','jsondata'='object'} into table t;
select 1;
select count(*) from t;
load data local infile {'filepath'='$resources/load_data/issue_32453_local_explain.jsonl','format'='jsonline','jsondata'='object'} into table t;
select id, s from t order by s;
drop database issue_32453_local_explain;
