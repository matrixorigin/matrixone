-- #29274: a fulltext2 BOOLEAN-mode CJK operand with a trailing `*` kept only the first trigram and
-- prefix-matched it, so `苹果香蕉*` wrongly matched 苹果香瓜 (shares 苹果香 but diverges at rune 4).
-- All ngrams fully contained in the prefix must be required (the same positional phrase the
-- unstarred operand builds); only a stem shorter than one trigram is a prefix.
drop database if exists ft2_cjk_star;
create database ft2_cjk_star;
use ft2_cjk_star;
set experimental_fulltext2_index = 1;

create table ft(id int primary key, body varchar(200));
insert into ft values (1,'苹果香蕉'),(2,'苹果香瓜'),(3,'苹果香蕉西瓜'),(4,'红苹果甜');
create fulltext2 index fi on ft(body) with parser ngram;

-- The bug: trailing * on a 4-rune stem must NOT return doc 2 (苹果香瓜). Expect 1, 3.
select id from ft where match(body) against('苹果香蕉*' in boolean mode) order by id;
select id from ft where match(body) against('+苹果香蕉*' in boolean mode) order by id;

-- Control: the unstarred phrase already returned the correct set.
select id from ft where match(body) against('+苹果香蕉' in boolean mode) order by id;
-- Control: natural-language agrees.
select id from ft where match(body) against('苹果香蕉') order by id;

-- A stem == one trigram: `苹果香*` matches every doc containing 苹果香 (1,2,3).
select id from ft where match(body) against('苹果香*' in boolean mode) order by id;
-- A stem shorter than one trigram keeps genuine prefix behavior: `苹果*` matches all docs with a
-- token prefixed by 苹果, including 红苹果甜's 苹果甜. Expect 1,2,3,4.
select id from ft where match(body) against('苹果*' in boolean mode) order by id;

-- #29274 P2: a stem MIXING CJK + a Latin tail. The head trigrams stay exact-positional, but the
-- FINAL Latin token must prefix-match the stored token (which can be longer than the stem). Before
-- the fix `苹果香蕉hell*` looked up the exact term hell, which the index never stored, and returned
-- nothing.
create table ftm(id int primary key, body varchar(200));
insert into ftm values
  (1,'苹果香蕉hello'),(2,'苹果香瓜hello'),(3,'苹果香蕉world'),(4,'苹果香蕉help'),(5,'苹果香蕉helloworld');
create fulltext2 index fim on ftm(body) with parser ngram;
-- positive: the Latin tail prefix-matches hello/helloworld while the CJK head pins 苹果香蕉. Expect 1,5.
select id from ftm where match(body) against('苹果香蕉hell*' in boolean mode) order by id;
select id from ftm where match(body) against('+苹果香蕉hell*' in boolean mode) order by id;
-- divergent CJK head (香瓜) is still excluded even though its tail prefix-matches: same prefix on the
-- divergent head selects only doc 2.
select id from ftm where match(body) against('苹果香瓜hell*' in boolean mode) order by id;
-- the tail is a real prefix, not presence-anywhere: help* matches help (4), not hello.
select id from ftm where match(body) against('苹果香蕉help*' in boolean mode) order by id;
-- a final Latin prefix present in no document matches nothing.
select id from ftm where match(body) against('苹果香蕉xyz*' in boolean mode) order by id;
-- contrast: the unstarred exact phrase looks up the whole token, so it matches only doc 1 (hello),
-- not the longer helloworld (5).
select id from ftm where match(body) against('苹果香蕉hello') order by id;

-- gojieba: same class of bug via word segmentation. 苹果香蕉 -> words [苹果, 香蕉]; a trailing * kept
-- only 苹果 and matched every 苹果* doc. A multi-word stem must require all words; a single-word stem
-- keeps its word prefix.
create table ftg(id int primary key, body varchar(200));
insert into ftg values (1,'苹果香蕉'),(2,'苹果香瓜'),(3,'苹果香蕉西瓜'),(4,'红苹果甜');
create fulltext2 index fig on ftg(body) with parser gojieba;
-- 苹果香蕉* : expect 1,3 (was 1,2,3,4).
select id from ftg where match(body) against('苹果香蕉*' in boolean mode) order by id;
select id from ftg where match(body) against('+苹果香蕉*' in boolean mode) order by id;
-- control: unstarred phrase.
select id from ftg where match(body) against('+苹果香蕉' in boolean mode) order by id;
-- single-word stem keeps its word prefix: expect 1,2,3,4.
select id from ftg where match(body) against('苹果*' in boolean mode) order by id;

-- gojieba review regression: the FINAL slot can be a PARTIAL Chinese word the * cut. 苹果香* segments
-- to 苹果 + 香, but docs stored 苹果 + 香蕉/香瓜; the final CJK word must prefix-match (not stay exact)
-- while the head 苹果 stays positional. Was EMPTY on the head. Expect 1,2,3 (香蕉 and 香瓜 both prefix
-- 香); doc 4 (红苹果甜) is excluded because 苹果 is not the first word.
select id from ftg where match(body) against('苹果香*' in boolean mode) order by id;
select id from ftg where match(body) against('+苹果香*' in boolean mode) order by id;

drop database ft2_cjk_star;
