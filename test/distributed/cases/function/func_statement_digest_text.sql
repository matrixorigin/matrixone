-- STATEMENT_DIGEST_TEXT issue example and core text normalization.
drop database if exists statement_digest_text_test;
create database statement_digest_text_test;
use statement_digest_text_test;

select statement_digest_text('SELECT 1') as digest;
select statement_digest_text('select 2 /* comment */ where 10=20') as digest;
select statement_digest_text('SELECT * FROM t WHERE id IN (1,2,3)') as digest;
select statement_digest_text(NULL) is null as digest_is_null;

create table statement_digest_text_input(id int primary key, sql_text text);
insert into statement_digest_text_input values
    (1, 'SELECT 1'),
    (2, 'SELECT 2 WHERE 3=4'),
    (3, NULL);
select id, statement_digest_text(sql_text) as digest
from statement_digest_text_input order by id;

drop database statement_digest_text_test;
