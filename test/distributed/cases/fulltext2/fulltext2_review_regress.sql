-- Regression cases for the three self-review blockers on the fulltext2 INCLUDE / box-free
-- covered path. Each previously produced a wrong result, a query error, or a panic.
set experimental_fulltext2_index = 1;
drop database if exists ft2_review;
create database ft2_review;
use ft2_review;

-- B3: an INCLUDE column whose name collides with the fulltext2_search TVF's reserved output
-- names (doc_id / score). Before the fix the covered-path name classifier misrouted it
-- (the include vector got the relevance score, leaving a length-0 vector -> shuffle PANIC).
-- After the fix the TVF's built-in outputs use reserved __mo_ft_* aliases, so the include
-- column keeps its user name and returns its STORED value.
create table b3 (id bigint primary key, body text not null, score int, doc_id int);
insert into b3 values (1,'quick brown fox',111,7),(2,'lazy fox sleeps',222,8),(3,'quick fox',333,9);
create fulltext2 index b3idx on b3 (body) include (score, doc_id);
select id, score from b3 where match(body) against('fox') order by id;
select id, doc_id from b3 where match(body) against('fox') order by id;
select id, score, doc_id from b3 where match(body) against('fox') order by id;
-- also with a LIMIT (non-streaming covered path) and a filter on the colliding include col
select id, score, doc_id from b3 where match(body) against('fox') and score > 150 order by id limit 5;

-- B2: a pk predicate on a BIT primary key gets peeled into the covered TVF prefilter. Before
-- the fix the evaluator rejected T_bit ("col -1 type not comparable") and the whole query
-- failed; now BIT is evaluated on the unsigned path.
create table b2 (bk bit(16) primary key, body text not null, prio int);
insert into b2 values (5,'quick brown fox',10),(6,'lazy fox sleeps',20),(7,'quick fox',30);
create fulltext2 index b2idx on b2 (body) include (prio);
select cast(bk as unsigned) as k, prio from b2 where match(body) against('fox') and bk = 7 order by k;
select cast(bk as unsigned) as k, prio from b2 where match(body) against('fox') and bk in (5,7) order by k;
select cast(bk as unsigned) as k, prio from b2 where match(body) against('fox') and bk > 5 order by k;

-- B1: a reused prepared MATCH with a pushed LIMIT, executed with a non-NULL pattern then a
-- NULL pattern. Before the fix the NULL execution re-emitted the previous execution's rows
-- (the reused output buffer was not cleared on the NULL-pattern early return); now NULL
-- returns the empty set.
create table b1 (id bigint primary key, body text not null);
insert into b1 values (1,'quick brown fox'),(2,'lazy fox sleeps'),(3,'quick fox');
create fulltext2 index b1idx on b1 (body);
prepare pb1 from 'select id from b1 where match(body) against(?) order by id limit 5';
set @q = 'fox';
execute pb1 using @q;
set @q = null;
execute pb1 using @q;
set @q = 'lazy';
execute pb1 using @q;
deallocate prepare pb1;

drop database ft2_review;
