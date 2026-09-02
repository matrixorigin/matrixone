-- STATEMENT_DIGEST_TEXT basic MySQL-compatible normalization.
drop database if exists statement_digest_text_test;
create database statement_digest_text_test;
use statement_digest_text_test;

select statement_digest_text('SELECT 1') as digest;
select statement_digest_text('SELECT 2 /* comment */ WHERE 10=20') as digest;
select statement_digest_text('SELECT a + b, a - b FROM t1,t2,t3 WHERE a=c') as digest;
select statement_digest_text('SELECT 1,2,3') as digest;
select statement_digest_text('INSERT INTO t VALUES (1,2),(3,4)') as digest;
select statement_digest_text('SELECT TRUE,FALSE,NULL') as digest;
select statement_digest_text('SELECT * FROM t WHERE a IS NULL OR b IS NOT NULL') as digest;
select statement_digest_text('SELECT 数量 FROM 订单 WHERE 编号 = 42') as digest;
select statement_digest_text('SELECT (1), ((1)), ABS(1), COALESCE(1)') as digest;
select statement_digest_text('SELECT DISTINCT a, CURRENT_TIMESTAMP FROM t WHERE a = ANY (SELECT b FROM u)') as digest;
select statement_digest_text('SELECT _utf8''a'', _latin1''b'', _foo''c''') as digest;
select statement_digest_text('SELECT /*+ INDEX(@qb t idx) */ * FROM t') as digest;
select statement_digest_text('CREATE TABLE t(a INT NULL, b INT DEFAULT NULL)') as digest;
select statement_digest_text(NULL) is null as digest_is_null;

-- Exercise non-constant vectors (the text protocol table path).
drop table if exists statement_digest_text_input;
create table statement_digest_text_input(id int primary key, sql_text text);
insert into statement_digest_text_input values
    (1, 'SELECT 1'),
    (2, 'SELECT 2 WHERE 3=4'),
    (3, NULL);
select id, statement_digest_text(sql_text) as digest
from statement_digest_text_input order by id;

-- Exercise a binary-protocol parameter and SQL NULL propagation.
prepare statement_digest_text_stmt from 'select statement_digest_text(?) as digest';
set @digest_sql = 'SELECT 3 WHERE 4=5';
execute statement_digest_text_stmt using @digest_sql;
set @digest_sql = NULL;
execute statement_digest_text_stmt using @digest_sql;
set @digest_sql = 'SELECT ?';
execute statement_digest_text_stmt using @digest_sql;
set @digest_sql = 'SELECT 6 WHERE 7=8';
execute statement_digest_text_stmt using @digest_sql;
deallocate prepare statement_digest_text_stmt;

-- MySQL rejects empty, malformed, multiple statements, and parameter markers
-- inside the statement being normalized.
select statement_digest_text('');
select statement_digest_text('SELECT');
select statement_digest_text('SELECT 1; SELECT 2');
select statement_digest_text('SELECT ?');

drop table statement_digest_text_input;
drop database statement_digest_text_test;
