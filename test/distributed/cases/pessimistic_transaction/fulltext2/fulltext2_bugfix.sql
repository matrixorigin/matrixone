-- BVT for fulltext2 CDC/rebuild correctness fixes (self-review findings):
--   #1 temporal/decimal PK: CDC INSERT must NOT panic encodePk — the consumer now
--      extracts PKs as native Go values (ReprNative), not SQL-display strings.
--   #3 T_json column via CDC is searchable — each json column is flattened
--      per-column, binary-aware (bytejson.ByteJson), matching the CREATE build.
--   #4 multi-column json via CDC is searchable on EVERY column — per-column flatten
--      instead of joining raw columns then flattening the blob once (zero tokens).
--   #6 REBUILD over an emptied table drops the stale tag=0 base — no longer serves
--      deleted docs when the rebuild sees zero source rows.
-- All four are CDC/async-path bugs, so the index is created on an empty table and the
-- rows flow in through ISCP CDC; sleep() waits for CDC to settle.
set experimental_fulltext2_index = 1;
drop database if exists ft2_bugfix;
create database ft2_bugfix;
use ft2_bugfix;

-- #1: datetime PK — CDC path (index created before the rows exist)
create table pk_dt (id datetime primary key, body text);
create fulltext2 index ftidx on pk_dt(body);
insert into pk_dt values ('2020-01-02 03:04:05','alpha beta'), ('2021-06-07 08:09:10','beta gamma');

-- #1: decimal PK
create table pk_dec (id decimal(18,4) primary key, body text);
create fulltext2 index ftidx on pk_dec(body);
insert into pk_dec values (123.4567,'alpha beta'), (890.1234,'beta gamma');

-- #3: single T_json column
create table j1 (id bigint primary key, doc json);
create fulltext2 index ftidx on j1(doc) with parser json;
insert into j1 values (1,'{"title":"quantum physics","tag":"science"}'), (2,'{"title":"cooking recipes","tag":"food"}');

-- #4: multi-column json
create table j2 (id bigint primary key, a json, b json);
create fulltext2 index ftidx on j2(a,b) with parser json;
insert into j2 values (1,'{"x":"hello world"}','{"y":"foo bar"}'), (2,'{"x":"lorem ipsum"}','{"y":"dolor sit"}');

-- wait for the initial CDC drain of all four tables
select sleep(45);

-- #1: both temporal/decimal-PK tables searchable => the consumer did not crash
select id from pk_dt where match(body) against('beta') order by id;
select id from pk_dt where match(body) against('gamma') order by id;
select id from pk_dec where match(body) against('beta') order by id;

-- #3: T_json flattened + searchable (both values and both docs)
select id from j1 where match(doc) against('quantum') order by id;
select id from j1 where match(doc) against('food') order by id;

-- #4: BOTH json columns searchable (a and b)
select id from j2 where match(a,b) against('hello') order by id;
select id from j2 where match(a,b) against('bar') order by id;
select id from j2 where match(a,b) against('dolor') order by id;

-- #6: REBUILD over an emptied table must not serve the stale (deleted) base
create table reb (id bigint primary key, body text);
create fulltext2 index ftidx on reb(body);
insert into reb values (1,'stale zebra'),(2,'stale zebra');
select sleep(45);
-- both rows searchable before the rebuild
select count(*) from reb where match(body) against('zebra');
-- empty the table, then REBUILD (rebuilds the base from the now-empty source)
delete from reb;
alter table reb alter reindex ftidx fulltext2;
-- the stale base is cleared => zero matches (before the fix this returned 2)
select count(*) from reb where match(body) against('zebra');

drop database ft2_bugfix;
