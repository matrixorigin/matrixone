-- REPLACE INTO must keep a fulltext index consistent, the same way INSERT does:
-- a row inserted or whose indexed text is changed via REPLACE has to be
-- immediately reflected in a MATCH ... AGAINST search, and the stale tokens of a
-- replaced row must no longer match.

drop database if exists ft_replace;
create database ft_replace;
use ft_replace;

create table t(id int primary key, body text);
insert into t values(1,'apple banana'),(2,'cherry date'),(3,'elderberry fig');
create fulltext index ftidx on t(body);

-- REPLACE a brand-new PK row (= insert): its tokens must be searchable.
replace into t values(5,'grape kiwi');
select id from t where match(body) against('grape') order by id;
select id from t where match(body) against('kiwi') order by id;

-- REPLACE an existing row's text: the new tokens match, the stale tokens do not.
replace into t values(1,'mango melon');
select id from t where match(body) against('mango') order by id;
select id from t where match(body) against('apple') order by id;
select id from t where match(body) against('banana') order by id;

-- Multi-row REPLACE mixing new and existing rows.
replace into t values(2,'mango pear'),(7,'apple quince'),(3,'mango plum');
select id from t where match(body) against('mango') order by id;
select id from t where match(body) against('apple') order by id;
select id from t where match(body) against('cherry') order by id;

select id, body from t order by id;

drop database if exists ft_replace;
