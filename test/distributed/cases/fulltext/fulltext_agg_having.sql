-- #29065: a grouped query whose only MATCH is an aggregate, filtered by a membership-implying
-- HAVING (MAX(MATCH)>0, or its alias score>0), must drive the fulltext index instead of erroring
-- 20105. The scan is INNER-joined to the matchers before aggregation, which is result-preserving for
-- MAX/SUM. MIN/AVG and non-membership comparisons are NOT driven (they would change the result), and
-- an aggregate MATCH with no membership HAVING stays refused.
set experimental_fulltext_index = 1;
drop database if exists ft_agg_having;
create database ft_agg_having;
use ft_agg_having;

create table docs(id int primary key, body text);
insert into docs values
  (1,'alpha alpha common'),
  (2,'alpha common'),
  (3,'beta common'),
  (4,'alpha beta'),
  (5,'gamma'),
  (6,'alpha alpha alpha');
create fulltext index ft on docs(body);

-- control: direct HAVING MATCH()>0 already works -> ids 1,2,4,6
select id from docs group by id, body having match(body) against('alpha') > 0 order by id;

-- A: HAVING on the aggregate alias -> ids 1,2,4,6
select id, max(match(body) against('alpha')) as score
from docs group by id, body having score > 0 order by id;

-- B: HAVING on the direct aggregate -> ids 1,2,4,6
select id, max(match(body) against('alpha')) as score
from docs group by id, body having max(match(body) against('alpha')) > 0 order by id;

-- SUM is also a safe driver -> ids 1,2,4,6
select id from docs group by id, body having sum(match(body) against('alpha')) > 0 order by id;

-- the served aggregate score is genuinely used, not a full scan.
-- @separator:table
-- @regex("fulltext_index_scan", true)
explain select id, max(match(body) against('alpha')) as score
from docs group by id, body having score > 0 order by id;

-- MIN(MATCH)>0 is NOT membership-implying (it requires EVERY row to match); left refused.
select id from docs group by id, body having min(match(body) against('alpha')) > 0 order by id;

-- Co-aggregate safety: driving drops non-matching rows before aggregation, so a query that also
-- aggregates over those rows (COUNT(*), or an aggregate over another column) must NOT be driven --
-- otherwise a multi-row group would silently count/aggregate matchers only. Left refused.
create table cat_docs(id int primary key, cat int, body text);
insert into cat_docs values (1,10,'alpha x'),(2,10,'alpha y'),(3,10,'nomatch'),(4,20,'alpha z'),(5,20,'beta only');
create fulltext index cft on cat_docs(body);
select cat, count(*) as c, max(match(body) against('alpha')) as s
from cat_docs group by cat having max(match(body) against('alpha')) > 0 order by cat;
select cat, max(id) as mx, max(match(body) against('alpha')) as s
from cat_docs group by cat having s > 0 order by cat;

-- #29065 P1: multiple DISTINCT aggregate MATCHes are NOT doc-level-intersection safe. Driving
-- INNER-joins the alpha and beta streams by doc_id, demanding one document match BOTH; but the
-- HAVING is satisfied when alpha and beta occur on DIFFERENT rows of the group. cat 10 (alpha on
-- id 1, beta on id 2) satisfies it, yet the intersection is empty. Refused (20105) rather than
-- silently dropping cat 10.
create table md(id int primary key, cat int, body text);
insert into md values (1,10,'alpha'),(2,10,'beta'),(3,20,'alpha'),(4,20,'alpha');
create fulltext index mdft on md(body);
select cat, max(match(body) against('alpha')) a, max(match(body) against('beta')) b
from md group by cat having a > 0 and b > 0 order by cat;

-- #29065 P1: an outer filter AFTER a WINDOW is NOT the AGG's HAVING. Harvesting `score > 0` here
-- would drive the index below the AGG and drop the non-matching group before ROW_NUMBER() is
-- computed, silently shifting rn. Refused (20105).
create table wd(id int primary key, cat int, body text);
insert into wd values (1,1,'nomatch'),(2,2,'alpha'),(3,3,'alpha');
create fulltext index wdft on wd(body);
select cat, score, rn from (
  select cat, max(match(body) against('alpha')) score, row_number() over (order by cat) rn
  from wd group by cat
) q where score > 0 order by cat;

-- Positive control: a REAL HAVING below the window still drives; ROW_NUMBER() runs over the
-- matching groups only (group set is preserved) -> (cat 2, rn 1), (cat 3, rn 2).
select cat, score, rn from (
  select cat, max(match(body) against('alpha')) score, row_number() over (order by cat) rn
  from wd group by cat having max(match(body) against('alpha')) > 0
) q order by cat;

drop database ft_agg_having;
