-- Issue #23024: scalar normalization and NULL propagation.
select statement_digest('SELECT 1') as digest;
select statement_digest('select 2') as digest;
select statement_digest(NULL) as digest;
select statement_digest('SELECT * FROM mytable WHERE cola = 10 AND colb = 20') as digest;
select statement_digest('SELECT * FROM t WHERE id IN (1,2,3)') as digest;
-- Unary signs are context-sensitive in MySQL's token digest.
select statement_digest('SELECT -42') as absorbed_sign_digest;
select statement_digest('SELECT * FROM t WHERE b=-42') as preserved_sign_digest;
select statement_digest('SELECT CASE WHEN a THEN -1 ELSE +2 END') as branch_sign_digest;
-- Charset introducers and statement terminators contribute digest tokens.
select statement_digest('SELECT _utf8mb4''hello''') as charset_digest;
select statement_digest('SELECT 1;') as semicolon_digest;
select statement_digest('/* comment only */') as comment_only_digest;
-- Counterexamples found by differential testing against MySQL 8.4.
select statement_digest('SELECT /*+ SET_VAR(sort_buffer_size=16M) */ 1') as hint_scaled_number_digest;
select statement_digest('SELECT a FROM t GROUP BY a WITH ROLLUP') as with_rollup_digest;
select statement_digest('CREATE TABLE t(a INT NULL, b INT NOT NULL, c INT DEFAULT NULL, d INT NULL DEFAULT NULL)') as ddl_null_digest;

-- Vector execution must preserve row-local NULL and input semantics.
drop database if exists statement_digest_23024;
create database statement_digest_23024;
use statement_digest_23024;
create table statement_digest_inputs_23024(sql_text text);
insert into statement_digest_inputs_23024 values
    ('SELECT 1'),
    ('select 2'),
    ('SELECT ''中文'''),
    (NULL);
select sql_text, statement_digest(sql_text) as digest from statement_digest_inputs_23024 order by sql_text;

-- Invalid and multi-statement arguments are rejected by the SQL parser.
select statement_digest('   ') as digest;
select statement_digest('SELECT FROM') as digest;
select statement_digest('SELECT 1; SELECT 2') as digest;

drop database statement_digest_23024;
