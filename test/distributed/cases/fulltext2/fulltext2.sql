-- fulltext2 (WAND positional engine) SYNCHRONOUS functional test: INSERT rows,
-- then CREATE FULLTEXT2 INDEX (builds the tag=0 base from source). No post-create
-- DML here — async CDC maintenance is covered by
-- pessimistic_transaction/fulltext2. NL mode is EXACT PHRASE (fulltext2's
-- distinguishing semantics); boolean supports the full +/-/~/</>/()/*/"..." surface.

drop database if exists test_fulltext2;
create database test_fulltext2;
use test_fulltext2;

-- CREATE FULLTEXT2 INDEX is gated behind experimental_fulltext2_index (default off).
set experimental_fulltext2_index = 1;

-- fulltext2 has its own relevance var (default BM25, distinct from classic
-- ft_relevancy_algorithm = TF-IDF).
select @@ft2_relevancy_algorithm;

-- ================= English: NL exact-phrase + boolean =================
drop table if exists docs;
create table docs(id bigint primary key, body text);
insert into docs values
 (0,'the quick brown fox jumps'),
 (1,'a quick brown dog'),
 (2,'the lazy fox sleeps'),
 (3,'brown bear and lazy cat'),
 (4,'quick quick quick fox fox');
create fulltext2 index ft on docs(body);
show create table docs;

-- NL EXACT-PHRASE: contiguous only.
select id from docs where match(body) against('quick brown fox') order by id;
select id from docs where match(body) against('brown fox') order by id;
select id from docs where match(body) against('lazy') order by id;
select id from docs where match(body) against('unicorn') order by id;

-- boolean mode.
select id from docs where match(body) against('+quick +fox' in boolean mode) order by id;
select id from docs where match(body) against('+quick -fox' in boolean mode) order by id;
select id from docs where match(body) against('quic*' in boolean mode) order by id;
select id from docs where match(body) against('"quick brown"' in boolean mode) order by id;
select id from docs where match(body) against('+brown +(fox dog)' in boolean mode) order by id;
select id from docs where match(body) against('lazy fox' in boolean mode) order by id;

-- ~ downweights: doc 3 (lazy, no fox) outranks doc 2 (lazy + fox). The planner
-- auto-sorts by score desc, so no ORDER BY is needed (result order IS score desc).
select id from docs where match(body) against('+lazy ~fox' in boolean mode);

-- match in aggregate.
select count(*) from docs where match(body) against('+quick +fox' in boolean mode);

-- ================= multi-column =================
drop table if exists mc;
create table mc(id bigint primary key, body text, title text);
insert into mc values (0,'quick brown fox','red car'),(1,'lazy dog','quick red');
create fulltext2 index ft on mc(body,title);
select id from mc where match(body,title) against('+quick +red' in boolean mode) order by id;
select id from mc where match(body,title) against('red') order by id;

-- ================= composite primary key =================
drop table if exists cp;
create table cp(id1 varchar, id2 bigint, body text, primary key(id1,id2));
insert into cp values ('a',1,'quick brown fox'),('b',2,'lazy fox sleeps');
create fulltext2 index ft on cp(body);
select id1,id2 from cp where match(body) against('+fox -brown' in boolean mode) order by id1;

-- ================= TF-IDF vs BM25 (membership stable) =================
set ft2_relevancy_algorithm='TF-IDF';
select id from docs where match(body) against('+lazy ~fox' in boolean mode);
set ft2_relevancy_algorithm='BM25';

drop database test_fulltext2;
