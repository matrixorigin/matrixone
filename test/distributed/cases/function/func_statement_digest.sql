-- MatrixOne build-scoped statement hash: canonical AST deparse, not MySQL token digest.
select length(statement_digest('SELECT 1')) as digest_length;
select statement_digest('SELECT 1') = statement_digest(' select /* ignored */ 1; ') as formatting_normalized;
select statement_digest('SELECT 1') <> statement_digest('SELECT 2') as literal_values_distinct;
select statement_digest('SELECT a') <> statement_digest('SELECT ''a''') as identifier_and_literal_distinct;
select statement_digest('SELECT @MiXeD') = statement_digest('SELECT @mixed') as user_variable_case_normalized;
select statement_digest(NULL) is null as null_propagates;

-- Parser-produced statement shapes that previously exercised formatting edges.
select statement_digest('SELECT TRIM(BOTH ''x'' FROM ''xxx'')') is not null as trim_formats;
select statement_digest('SELECT (SELECT 1)') is not null as nested_query_formats;
select statement_digest('PREPARE p FROM ''SELECT 1''') is not null as prepare_formats;

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
