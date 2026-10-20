-- #30402: fulltext2_search_df_cutoff drops a ranked bag-of-words term from the BM25
-- search when its GLOBAL document frequency reaches the configured fraction of the
-- corpus (df/N >= cutoff). The term's idf = log10(N/df) is near 0, so it contributes
-- almost nothing to the score yet owns the longest posting list in the query; dropping
-- it skips that posting list entirely. 0 (the default) disables the cutoff.
--
-- 'rare' is in docs 1..3 at DISTINCT term frequencies (3,2,1) -> df=3, strictly ordered
-- top-3. 'common' is in docs 1..8 -> df=8 of N=10 (df/N=0.8). A query of 'rare common':
--   * cutoff off: matches every doc that has EITHER term (ids 1..8) and 'common'
--     contributes to the score of ids 1..3.
--   * cutoff 0.7: drops 'common' (0.8 >= 0.7), so the query reduces to 'rare' -- only
--     ids 1..3 match, and their scores drop the 'common' contribution.
-- The top-3 ids stay {1,2,3} (strict 'rare' ordering) but their SCORES differ, and the
-- total match count drops from 8 to 3 -- a deterministic before/after of the cutoff.
set experimental_fulltext2_index = 1;
drop database if exists ft2_cutoff;
create database ft2_cutoff;
use ft2_cutoff;

-- default is 0 (cutoff disabled), settable in [0,1].
select @@fulltext2_search_df_cutoff as default_val;

create table t(id bigint primary key, body text);
insert into t values
  (1,'rare rare rare common'),
  (2,'rare rare common'),
  (3,'rare common'),
  (4,'common data text'),
  (5,'common data text'),
  (6,'common data text'),
  (7,'common data text'),
  (8,'common data text'),
  (9,'data text nine'),
  (10,'data text ten');
create fulltext2 index ft on t(body);

-- CUTOFF OFF (default 0): 'common' is kept, so every doc with either term matches.
select id, match(body) against('rare common' in bm25 mode) as score
  from t where match(body) against('rare common' in bm25 mode)
  order by score desc limit 3;
select count(*) as match_count_off
  from t where match(body) against('rare common' in bm25 mode);

-- CUTOFF ON (0.7): df(common)/N = 0.8 >= 0.7, so 'common' is dropped from the search.
-- The query reduces to 'rare': same top-3 ids, scores without the 'common' contribution,
-- and only the 3 'rare' docs match.
set fulltext2_search_df_cutoff = 0.7;
select @@fulltext2_search_df_cutoff as after_set;
select id, match(body) against('rare common' in bm25 mode) as score
  from t where match(body) against('rare common' in bm25 mode)
  order by score desc limit 3;
select count(*) as match_count_on
  from t where match(body) against('rare common' in bm25 mode);

set fulltext2_search_df_cutoff = 0;
drop database ft2_cutoff;
