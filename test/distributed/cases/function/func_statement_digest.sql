-- MatrixOne build-scoped statement hash: canonical AST deparse, not MySQL token digest.
select length(statement_digest('SELECT 1')) as digest_length;
select if(statement_digest('SELECT 1') = statement_digest(' select /* ignored */ 1; '), 'true', 'false') as formatting_normalized;
select if(statement_digest('SELECT 1') <> statement_digest('SELECT 2'), 'true', 'false') as literal_values_distinct;
select if(statement_digest('SELECT a') <> statement_digest('SELECT ''a'''), 'true', 'false') as identifier_and_literal_distinct;
select if(statement_digest('SELECT @MiXeD') = statement_digest('SELECT @mixed'), 'true', 'false') as user_variable_case_normalized;
select if(statement_digest(NULL) is null, 'true', 'false') as null_propagates;

-- A rejected statement is an error, never a digest of empty or partial output.
-- The successful query after the first error proves the SQL session remains usable.
-- @regex("parser|syntax|empty|eof", true)
select statement_digest('SELECT FROM');
select length(statement_digest('SELECT 1')) as digest_after_parse_error;
-- @regex("parser|syntax|empty|eof", true)
select statement_digest('SELECT 1; SELECT 2');
-- @regex("parser|syntax|empty|eof", true)
select statement_digest('');
-- @regex("parser|syntax|empty|eof", true)
select statement_digest('/* comment only */');

-- Parser-produced statement shapes that previously exercised formatting edges.
select if(statement_digest('SELECT TRIM(BOTH ''x'' FROM ''xxx'')') is not null, 'true', 'false') as trim_formats;
select if(statement_digest('SELECT (SELECT 1)') is not null, 'true', 'false') as nested_query_formats;
select if(statement_digest('PREPARE p FROM ''SELECT 1''') is not null, 'true', 'false') as prepare_formats;

-- Values supplied by rows use the same scalar/null behavior as constants.
drop database if exists statement_digest_23024;
create database statement_digest_23024;
use statement_digest_23024;
create table statement_digest_inputs_23024(sql_text text);
insert into statement_digest_inputs_23024 values
    ('SELECT 1'),
    ('select 2'),
    ('SELECT ''中文'''),
    (NULL);
select count(*) as rows, count(statement_digest(sql_text)) as hashed_rows from statement_digest_inputs_23024;
drop database statement_digest_23024;
select count(*) as leftover_statement_digest_databases
from mo_catalog.mo_database
where datname = 'statement_digest_23024';
