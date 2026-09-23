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

-- #29065 P1: a WRAPPED constant threshold must be EVALUATED, not have its wrapper input read.
-- floor(1e-1) evaluates to 0, so `>= floor(1e-1)` is the always-true `>= 0`: it cannot prove
-- membership, so it is REJECTED (20105), exactly like a bare `>= 0`. Reading the wrapper input (0.1)
-- as a positive threshold wrongly drove the index and silently dropped the zero-score groups
-- (ids 3,5) instead of rejecting.
select id from docs group by id, body having max(match(body) against('alpha')) >= floor(1e-1) order by id;

-- #29065 P1: an aggregate wrapper with a per-row / nullable extra argument is NOT drop-safe.
-- ROUND(MATCH,digits) returns NULL when digits is NULL. Driving drops non-matching rows before
-- aggregation, so a group whose kept (matching) row rounds to NULL and whose dropped (non-matching)
-- row rounded to a non-null 0 flips SUM([NULL,0])=0 into SUM([NULL])=NULL, silently losing the group.
-- cat 10 has (alpha,digits=NULL) matching and (beta,digits=0) non-matching -- exactly that shape.
create table rd(id int primary key, cat int, body text, digits int);
insert into rd values
  (1,10,'alpha',null),(2,10,'beta',0),
  (3,20,'alpha',2),(4,20,'gamma',0),
  (5,30,'beta gamma',0),(6,30,'gamma delta',0),
  (7,40,'delta beta',0),(8,40,'gamma beta',0);
create fulltext index rdft on rd(body);

-- Constant, non-null digits IS drop-safe: round(0,2)=0 is the SUM identity, so driving is
-- result-preserving and the index is used.
-- @separator:table
-- @regex("fulltext_index_scan", true)
explain select cat, sum(round(match(body) against('alpha'), 2)) as total
from rd group by cat having max(match(body) against('alpha')) > 0 order by cat;
select cat, sum(round(match(body) against('alpha'), 2)) as total
from rd group by cat having max(match(body) against('alpha')) > 0 order by cat;

-- Per-row (column) digits is NOT drop-safe -> refused (20105) rather than silently losing cat 10.
select cat, sum(round(match(body) against('alpha'), digits)) as total
from rd group by cat having max(match(body) against('alpha')) > 0 order by cat;

-- #29065 P1: a cast whose target type does NOT preserve zero breaks drop-safety and membership.
-- CAST(0 AS YEAR)=2000 and CAST(CAST(0 AS CHAR) AS YEAR)=2000, so a dropped non-matching row
-- (relevance 0) becomes a non-zero value. Driving would silently change results; conservatively
-- left at 20105 instead. cat 10 has (alpha) matching and (beta) non-matching.
create table castrd(id int primary key, cat int, body text);
insert into castrd values (1,10,'alpha'),(2,10,'beta'),(3,20,'gamma'),(4,30,'delta');
create fulltext index castft on castrd(body);

-- SUM over a cast chain that turns the dropped non-matching 0 into 2000 is NOT drop-safe -> 20105.
select cat, sum(cast(cast(match(body) against('alpha') as char) as year)) as s
from castrd group by cat having max(match(body) against('alpha')) > 0 order by cat;

-- A HAVING that casts MAX(MATCH) through SIGNED/CHAR/YEAR before > 0 is not plain membership
-- (a non-matching group's 0 casts to 2000 > 0 and must be kept) -> 20105, not a partial result.
select cat from castrd group by cat
having cast(cast(cast(max(match(body) against('alpha')) as signed) as char) as year) > 0 order by cat;

-- Control: a cast to a zero-preserving numeric type (DECIMAL) IS drop-safe -- CAST(0 AS DECIMAL)=0 is
-- the SUM identity -- so driving stays result-preserving and the index is used.
-- @separator:table
-- @regex("fulltext_index_scan", true)
explain select cat, sum(cast(match(body) against('alpha') as decimal(10,2))) as s
from castrd group by cat having max(match(body) against('alpha')) > 0 order by cat;
select cat, sum(cast(match(body) against('alpha') as decimal(10,2))) as s
from castrd group by cat having max(match(body) against('alpha')) > 0 order by cat;

drop database ft_agg_having;
