-- MatrixOne build-scoped statement hash: canonical AST deparse, not MySQL token digest.
select length(mo_statement_hash('SELECT 1')) as digest_length;
select if(mo_statement_hash('SELECT 1') = mo_statement_hash(' select /* ignored */ 1; '), 'true', 'false') as formatting_normalized;
select if(mo_statement_hash('SELECT 1') <> mo_statement_hash('SELECT 2'), 'true', 'false') as literal_values_distinct;
select if(mo_statement_hash('SELECT a') <> mo_statement_hash('SELECT ''a'''), 'true', 'false') as identifier_and_literal_distinct;
select if(mo_statement_hash('SELECT @MiXeD') = mo_statement_hash('SELECT @mixed'), 'true', 'false') as user_variable_case_normalized;
select if(mo_statement_hash(NULL) is null, 'true', 'false') as null_propagates;

-- A rejected statement is an error, never a digest of empty or partial output.
-- The successful query after the first error proves the SQL session remains usable.
-- @regex("parser|syntax|empty|eof", true)
select mo_statement_hash('SELECT FROM');
select length(mo_statement_hash('SELECT 1')) as digest_after_parse_error;
-- @regex("parser|syntax|empty|eof", true)
select mo_statement_hash('SELECT 1; SELECT 2');
-- @regex("parser|syntax|empty|eof", true)
select mo_statement_hash('');
-- @regex("parser|syntax|empty|eof", true)
select mo_statement_hash('/* comment only */');

-- Parser-produced statement shapes that previously exercised formatting edges.
select if(mo_statement_hash('SELECT TRIM(BOTH ''x'' FROM ''xxx'')') is not null, 'true', 'false') as trim_formats;
select if(mo_statement_hash('SELECT (SELECT 1)') is not null, 'true', 'false') as nested_query_formats;
select if(mo_statement_hash('PREPARE p FROM ''SELECT 1''') is not null, 'true', 'false') as prepare_formats;

-- Values supplied by rows use the same scalar/null behavior as constants.
drop database if exists mo_statement_hash_23024;
create database mo_statement_hash_23024;
use mo_statement_hash_23024;
create table mo_statement_hash_inputs_23024(sql_text text);
insert into mo_statement_hash_inputs_23024 values
    ('SELECT 1'),
    ('select 2'),
    ('SELECT ''中文'''),
    (NULL);
select count(*) as rows, count(mo_statement_hash(sql_text)) as hashed_rows from mo_statement_hash_inputs_23024;
drop database mo_statement_hash_23024;
select count(*) as leftover_mo_statement_hash_databases
from mo_catalog.mo_database
where datname = 'mo_statement_hash_23024';
