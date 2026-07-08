-- WAND "retrieval" fulltext index: LIMIT top-K pushdown vs. no-LIMIT (all matches).
-- With a LIMIT, the top-K is produced by the Block-Max WAND walk itself (score-ordered,
-- pruned) — the LIMIT is pushed into the WAND search. With no LIMIT, the walk returns
-- ALL matching docs, ranked. This case exercises BOTH paths; the LIMIT results are
-- deliberately NOT in id order, proving the top-K is ordered by SCORE, not id.
-- The index is async (built from postings via ISCP at CREATE); a sleep lets it finish.
drop database if exists ft_retrieval_limit;
create database ft_retrieval_limit;
use ft_retrieval_limit;

create table t (id bigint primary key, txt text);
-- All docs are 3 tokens (uniform length) so score follows the number of matched query
-- terms. For query '营养 早餐 视频': doc3 has all 3, doc2 has 2, doc4/doc1 have 1 each
-- (视频 has higher idf than 营养), so score(3) > score(2) > score(4) > score(1) — all
-- distinct (no ties), hence a deterministic top-K order that differs from id order.
insert into t values
 (1, '营养 天气 城市'),
 (2, '营养 早餐 城市'),
 (3, '营养 早餐 视频'),
 (4, '视频 天气 城市'),
 (5, '教育 成长 学习');

create fulltext index ft on t(txt) with parser retrieval;

-- wait for the async (CDC/ISCP) initial build from postings
select sleep(60);

-- no LIMIT: returns ALL matching docs (doc5 matches nothing). ordered by id for a stable check.
select id from t where match(txt) against('营养 早餐 视频' in retrieval mode) order by id;

-- LIMIT k: top-K via the WAND walk, in SCORE order (NOT id order -> proves ranking + pruning).
select id from t where match(txt) against('营养 早餐 视频' in retrieval mode) limit 1;
select id from t where match(txt) against('营养 早餐 视频' in retrieval mode) limit 2;
select id from t where match(txt) against('营养 早餐 视频' in retrieval mode) limit 3;

-- LIMIT larger than the match count returns all matches (ranked), not an error.
select id from t where match(txt) against('营养 早餐 视频' in retrieval mode) limit 10;

-- DEFAULT mode (no 'in retrieval mode') routes to the same WAND top-K.
select id from t where match(txt) against('营养 早餐 视频') limit 2;

-- single keyword: no LIMIT returns all matches (ranked); ordered by id for a stable check.
select id from t where match(txt) against('营养' in retrieval mode) order by id;

-- no-match term + LIMIT returns empty (not an error).
select id from t where match(txt) against('不存在' in retrieval mode) limit 5;

drop database ft_retrieval_limit;
