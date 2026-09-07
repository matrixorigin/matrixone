-- Copyright 2026 Matrix Origin
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Issue #28327: MATCH used only for ordering or scoring is optional. It must
-- not turn the base-table/fulltext rewrite into an INNER JOIN that drops rows
-- with relevance 0. MySQL's natural-language MATCH documentation shows the
-- score-only form retaining nonmatching rows with a zero score.
set ft_relevancy_algorithm="TF-IDF";
set experimental_fulltext_index = 1;

drop database if exists ft_score_rows_28327;
create database ft_score_rows_28327;
use ft_score_rows_28327;

create table docs(id int primary key, body text);
insert into docs values (1,'alpha'),(2,'beta'),(3,'alpha beta');
create fulltext index ft on docs(body);

-- ORDER BY-only MATCH preserves id 2, whose score is zero.
select id from docs order by match(body) against('alpha') desc, id;

-- Bare and wrapped projected scores expose zero rather than dropping id 2.
-- Use boolean score predicates instead of algorithm-specific float goldens.
select id, if(match(body) against('alpha') > 0, 1, 0) as has_score from docs order by id;
select id, if(round(match(body) against('alpha'), 3) > 0, 1, 0) as has_score from docs order by id;

-- A scalar score predicate and ORDER BY remain base-anchored when zero qualifies.
select id from docs
where round(match(body) against('alpha'), 3) >= 0
order by round(match(body) against('alpha'), 3), id;

-- A simple scalar base-table predicate must not make an optional score stream
-- become an INNER JOIN, and a query with no hits still returns every base row.
select id from docs where id > 1 order by match(body) against('alpha') desc, id;
select id from docs order by match(body) against('missing') desc, id;

-- LIMIT must apply after the complete base-anchored result is sorted.
select id from docs order by match(body) against('alpha') asc, id limit 2;
select id from docs order by match(body) against('alpha') desc, id limit 2;

-- Two distinct optional streams each retain their own zero scores.
select id,
       if(match(body) against('alpha') > 0, 1, 0) as alpha_score,
       if(match(body) against('beta') > 0, 1, 0) as beta_score
from docs order by id;

-- A required alpha stream filters, while an optional beta score still preserves
-- the alpha-only row with beta score zero.
select id, if(match(body) against('beta') > 0, 1, 0) as beta_score
from docs
where match(body) against('alpha')
order by match(body) against('beta') desc, id;

-- Existing bare positive WHERE behavior remains membership-filtered.
select id from docs where match(body) against('alpha') order by id;

drop database ft_score_rows_28327;
set ft_relevancy_algorithm="TF-IDF";
