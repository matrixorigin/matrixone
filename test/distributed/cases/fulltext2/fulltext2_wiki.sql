-- fulltext2 on a real 1K-doc Wikipedia corpus (English + Chinese), loaded from
-- $resources/fulltext2/wiki_{en,zh}_1k.csv. Unlike the tiny hand-written cases this
-- drives a realistic build (≈1M postings) so the segment build/spill and the
-- max_postings_capacity multi-segment seal are exercised on real text. All assertions
-- are COUNT-based (order-independent) so results are stable regardless of top-k tie
-- ordering. Sync-only (no async CDC) to keep it deterministic.

drop database if exists ft2_wiki;
create database ft2_wiki;
use ft2_wiki;
set experimental_fulltext2_index = 1;

-- ================= English (gojieba word tokens) =================
create table en(id bigint primary key, body text);
load data infile '$resources/fulltext2/wiki_en_1k.csv' into table en fields terminated by ',' enclosed by '"' lines terminated by '\n';
select count(*) docs from en;
create fulltext2 index ft on en(body) with parser gojieba;

-- NL single-term membership counts (deterministic).
select count(*) from en where match(body) against('anarchism');
select count(*) from en where match(body) against('philosophy');
select count(*) from en where match(body) against('government');
select count(*) from en where match(body) against('zzznotawordzzz');
-- boolean mode: both terms present / one excluded.
select count(*) from en where match(body) against('+state +government' in boolean mode);
select count(*) from en where match(body) against('+anarchism -philosophy' in boolean mode);

-- ================= Chinese (ngram / CJK) =================
create table zh(id bigint primary key, body text);
load data infile '$resources/fulltext2/wiki_zh_1k.csv' into table zh fields terminated by ',' enclosed by '"' lines terminated by '\n';
select count(*) docs from zh;
create fulltext2 index ft on zh(body) with parser ngram;

select count(*) from zh where match(body) against('数学');
select count(*) from zh where match(body) against('中国');
select count(*) from zh where match(body) against('历史');

-- ================= max_postings_capacity multi-segment transparency =================
-- A small posting cap seals the SAME corpus into many tag=0 base segments; every
-- COUNT must be identical to the single-segment English index above (the Index spans
-- segments). This exercises the posting-seal + per-segment spill on real data.
create table en_cap(id bigint primary key, body text);
insert into en_cap select * from en;
create fulltext2 index ft on en_cap(body) with parser gojieba max_postings_capacity 20000;
select count(*) from en_cap where match(body) against('anarchism');
select count(*) from en_cap where match(body) against('philosophy');
select count(*) from en_cap where match(body) against('+state +government' in boolean mode);

-- ================= REBUILD (reindex) leaves results unchanged =================
alter table en_cap alter reindex ft fulltext2;
select count(*) from en_cap where match(body) against('anarchism');
select count(*) from en_cap where match(body) against('+state +government' in boolean mode);

drop database ft2_wiki;
