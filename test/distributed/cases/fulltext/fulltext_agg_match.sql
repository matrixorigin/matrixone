-- Relevance algorithm pinned FIRST for deterministic scores (other cases in this suite change it).
set ft_relevancy_algorithm="TF-IDF";
-- #28681: a fulltext MATCH that appears INSIDE an aggregate expression -- max(match(...)),
-- group_concat(... order by match(...)) -- must be rewritten to the score column the
-- fulltext_index_scan produces, exactly as a MATCH in the projection or a wrapped scalar is.
-- Before the fix, applyIndicesForAggUsingFullTextIndex reparented the aggregate's child to the
-- index-scan join but left the raw fulltext_match in AggList, so it reached execution and threw
-- "MATCH() AGAINST() function cannot be replaced by FULLTEXT INDEX" (error 20105). Same filter
-- projected directly, or wrapped in a CTE, always worked -- so this asserts the in-aggregate forms
-- now match those controls, and EXPLAIN confirms the index is still reached (no silent full-scan).
set experimental_fulltext_index = 1;
drop database if exists ft_agg_match;
create database ft_agg_match;
use ft_agg_match;

create table docs(id int primary key, body text);
insert into docs values
 (1, 'hello world'),
 (2, 'foo bar'),
 (3, 'hello hello again'),
 (4, 'world only'),
 (5, 'hello matrix one'),
 (6, null);
create fulltext index ft_body on docs(body);

-- max(match(...)) directly inside the aggregate: returns the top score, does not error.
select max(match(body) against('hello')) as top from docs where match(body) against('hello');

-- group_concat over the match score as the aggregate argument.
select group_concat(match(body) against('hello') order by id separator '|') as scores from docs where match(body) against('hello');

-- group_concat ordered by the match score inside the aggregate's ORDER BY.
select group_concat(id order by match(body) against('hello') desc, id separator '|') as ids from docs where match(body) against('hello');

-- Control: the CTE form always worked. The in-aggregate ordering above must equal this.
with hits as (
  select id, match(body) against('hello') score from docs where match(body) against('hello')
)
select max(score) as top, group_concat(id order by score desc, id separator '|') as ids from hits;

-- GROUP BY match(...): the served MATCH lands in the aggregate node's GroupBy, a sibling of the
-- AggList repro above, and must be rewritten to the score column too (else it errors 20105 in the
-- GROUP BY clause). Group the two distinct hello scores (0.049216866 x2, 0.09843373 x1).
select match(body) against('hello') as score, count(*) as c from docs where match(body) against('hello') group by match(body) against('hello') order by score;

-- The index is still reached (the rewrite did not fall back to a full scan).
-- @separator:table
-- @regex("fulltext_index_scan", true)
explain select max(match(body) against('hello')) from docs where match(body) against('hello');

drop database ft_agg_match;
