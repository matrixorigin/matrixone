-- FULLTEXT2 search acceptance coverage for #27155 and #27245.
-- These SQL statements assert terminal semantics only. Typed route selection,
-- exact score bits, and lazy/block/liveness mechanics are asserted by the
-- package tests; this file does not infer a fast path from SQL result order.
set experimental_fulltext2_index = 1;
drop database if exists fulltext2_phrase_must;
create database fulltext2_phrase_must;
use fulltext2_phrase_must;

create table docs (
    id bigint primary key,
    body text not null,
    category varchar(20) not null,
    status int not null
);
insert into docs values
(1, 'quick brown fox', 'tech', 1),
(2, 'brown quick fox', 'tech', 1),
(3, 'quick red fox', 'tech', 0),
(4, 'quick brown dog', 'tech', 1),
(5, 'quick brown fox quick brown fox', 'ops', 1),
(6, 'fox quick brown', 'tech', 1),
(7, 'quick dog brown fox', 'tech', 1),
(8, 'quick fox', 'tech', 1),
(9, 'sleepy owl', 'ops', 1);
create fulltext2 index ft on docs (body) include (category, status);

-- #27155: ordered contiguous phrase, reversed/non-adjacent controls, repeated
-- phrase occurrences, and an absent-term empty intersection.
select id from docs where match(body) against('quick brown') order by id;
select id from docs where match(body) against('brown quick') order by id;
select id from docs where match(body) against('quick brown fox') order by id;
select id from docs where match(body) against('quick brown fox quick') order by id;
select id from docs where match(body) against('quick red') order by id;
select id from docs where match(body) against('quick missing') order by id;
select id from docs where match(body) against('quick') order by id;

-- #27245: atomic pure-MUST with duplicate and missing terms, plus terminal
-- WHERE/INCLUDE filters. The projected INCLUDE columns make the filter result
-- observable at the SQL boundary.
select id from docs where match(body) against('+quick +fox' in boolean mode) order by id;
select id from docs where match(body) against('+quick +quick +fox' in boolean mode) order by id;
select id from docs where match(body) against('+quick +missing' in boolean mode) order by id;
select id from docs where match(body) against('qui*' in boolean mode) order by id;
-- @separator:table
select id, category, status from docs where match(body) against('+quick +fox' in boolean mode) and category = 'tech' and status = 1 order by id;
-- @separator:table
select id, category, status from docs where match(body) against('+quick +fox' in boolean mode) and status = 0 order by id;
-- The LIMIT covers all five qualifying rows; the runner compares the full
-- unordered result set while preserving the no-ORDER-BY execution shape.
-- @separator:table
select id, category, status from docs where match(body) against('+quick +fox' in boolean mode) and category = 'tech' and status = 1 limit 5;

-- Complex Boolean controls must keep the existing evaluator: MUST-NOT,
-- group, phrase, and SHOULD/ADJUST all have terminal result assertions.
select id from docs where match(body) against('+quick -fox' in boolean mode) order by id;
select id from docs where match(body) against('+(quick brown) +fox' in boolean mode) order by id;
select id from docs where match(body) against('+quick "brown fox"' in boolean mode) order by id;
select id from docs where match(body) against('+quick +"brown fox"' in boolean mode) order by id;
select id from docs where match(body) against('+quick brown' in boolean mode) order by id;
select id from docs where match(body) against('+quick ~fox' in boolean mode) order by id;

-- The scorer may change, but pure-MUST membership is stable under both modes.
set ft2_relevancy_algorithm = 'TF-IDF';
select id from docs where match(body) against('+quick +fox' in boolean mode) and category = 'tech' order by id;
set ft2_relevancy_algorithm = 'BM25';
select id from docs where match(body) against('+quick +fox' in boolean mode) and category = 'tech' order by id;

drop database fulltext2_phrase_must;
