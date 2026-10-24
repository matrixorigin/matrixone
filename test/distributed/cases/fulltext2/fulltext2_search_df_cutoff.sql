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

-- Never-empty: the original terms are kept when the query has one term ('common', 8),
-- when every term would be dropped ('common data': data df 7/10 >= 0.7, 10 = the uncut
-- count), or when no surviving term occurs in the corpus ('common zzz', 8).
select count(*) as single_term from t where match(body) against('common' in bm25 mode);
select count(*) as all_dropped from t where match(body) against('common data' in bm25 mode);
select count(*) as survivor_absent from t where match(body) against('common zzz' in bm25 mode);

-- Boolean and natural-language queries are never cut: same counts as with the cutoff off.
select count(*) as boolean_required from t where match(body) against('+common' in boolean mode);
select count(*) as boolean_either from t where match(body) against('rare common' in boolean mode);
select count(*) as natural_language from t where match(body) against('rare common');

-- A filtered query can return fewer rows, including none: 'common' is cut before the
-- id > 3 filter, and only ids 1..3 have 'rare'.
select count(*) as filtered_on from t where match(body) against('rare common' in bm25 mode) and id > 3;

-- A term below the cutoff is kept: df(common)/N = 0.8 < 0.9.
set fulltext2_search_df_cutoff = 0.9;
select count(*) as kept_below from t where match(body) against('rare common' in bm25 mode);

-- A value outside (0, 1] is rejected and the setting is unchanged.
set fulltext2_search_df_cutoff = 1.5;
set fulltext2_search_df_cutoff = -0.1;
select @@fulltext2_search_df_cutoff as unchanged;

-- A change of value invalidates prepared plans; an unchanged re-set keeps the value.
set fulltext2_search_df_cutoff = 0;
select count(*) as filtered_off from t where match(body) against('rare common' in bm25 mode) and id > 3;
-- The uncut counts the cut queries above must equal.
select count(*) as all_dropped_off from t where match(body) against('common data' in bm25 mode);
select count(*) as boolean_required_off from t where match(body) against('+common' in boolean mode);
select count(*) as boolean_either_off from t where match(body) against('rare common' in boolean mode);
select count(*) as natural_language_off from t where match(body) against('rare common');
prepare s1 from 'select count(*) from t where match(body) against(''rare common'' in bm25 mode)';
execute s1;
set fulltext2_search_df_cutoff = 0.7;
execute s1;
set fulltext2_search_df_cutoff = 0.7;
execute s1;
set fulltext2_search_df_cutoff = 0;
execute s1;
deallocate prepare s1;

set fulltext2_search_df_cutoff = 0;
drop database ft2_cutoff;
