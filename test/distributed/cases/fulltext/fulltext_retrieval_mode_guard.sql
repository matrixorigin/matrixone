-- RETRIEVAL mode requires a retrieval-parser fulltext index. On a non-retrieval
-- (default/ngram) index it must be rejected at PLAN time with a clear message,
-- not fail at runtime with the opaque "invalid fulltext search mode". The classic
-- natural-language / boolean modes must still work on that same index.
drop database if exists ft_mode;
create database ft_mode;
use ft_mode;
create table t (id bigint primary key, txt text);
insert into t values (1,'apple banana'),(2,'banana cherry'),(3,'cherry date');
create fulltext index ft on t(txt);
-- RETRIEVAL mode on a non-retrieval index: rejected at plan time
select id from t where match(txt) against('apple' in retrieval mode) order by id;
-- DEFAULT (natural-language) mode still works
select id from t where match(txt) against('apple') order by id;
-- BOOLEAN mode still works
select id from t where match(txt) against('+apple' in boolean mode) order by id;
-- NATURAL LANGUAGE mode still works
select id from t where match(txt) against('apple' in natural language mode) order by id;
drop database ft_mode;
