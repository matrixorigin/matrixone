-- FULLTEXT2 safe candidate LIMIT coverage.
-- The exact BIGINT membership path may bound the TVF to LIMIT+OFFSET because
-- membership is exact and the Boolean query is pure MUST. Approximate Bloom
-- membership and every richer Boolean shape remain unbounded at the TVF.

set experimental_fulltext2_index = 1;
drop database if exists fulltext2_candidate_limit;
create database fulltext2_candidate_limit;
use fulltext2_candidate_limit;

create table ft_exact (
    id bigint primary key,
    body text not null,
    category varchar(20) not null
);
insert into ft_exact values
(1, 'needle needle needle needle needle high', 'drop'),
(2, 'needle needle medium', 'drop'),
(3, 'needle alpha', 'keep'),
(4, 'needle alpha beta', 'keep'),
(5, 'needle alpha beta gamma', 'keep'),
(6, 'background alpha beta gamma', 'other');
create fulltext2 index ft_exact_idx on ft_exact(body) with parser gojieba;

-- Two MATCH predicates over the same indexed column are a supported
-- multi-stream execution shape when their terms differ.  Keep the PK order
-- explicit so this semantic result is independent of score ties.
-- @sortkey:0
select id from ft_exact where match(body) against('+needle' in boolean mode) and match(body) against('+alpha' in boolean mode) and category = 'keep' order by id limit 2 offset 1;
-- The same multi-MATCH shapes with Bloom membership pushdown must not hang.
set fulltext_bloom_filter_pushdown = 1;
-- @sortkey:0
select id from ft_exact where match(body) against('+needle' in boolean mode) and match(body) against('+alpha' in boolean mode) and category = 'keep' order by id limit 2 offset 1;
-- @sortkey:0
select id from ft_exact where match(body) against('+needle' in boolean mode) and match(body) against('+alpha' in boolean mode) and match(body) against('+beta' in boolean mode) and category = 'keep' order by id;
set fulltext_bloom_filter_pushdown = 0;

-- With exact membership, an explicit score order bounds the search exactly like the
-- implicit relevance order, and returns the same page as the unbounded plan.
set fulltext_bloom_filter_pushdown = 1;
-- @regex("Candidate Limit: 3",true)
explain select id, category, match(body) against('+needle' in boolean mode) as sc from ft_exact where match(body) against('+needle' in boolean mode) and category = 'keep' order by sc desc limit 2 offset 1;
select id, category, match(body) against('+needle' in boolean mode) as sc from ft_exact where match(body) against('+needle' in boolean mode) and category = 'keep' order by sc desc limit 2 offset 1;
set fulltext_bloom_filter_pushdown = 0;
select id, category, match(body) against('+needle' in boolean mode) as sc from ft_exact where match(body) against('+needle' in boolean mode) and category = 'keep' order by sc desc limit 2 offset 1;

-- Ranking witness: the controlled TF/document-length fixture makes the two
-- excluded rows score strictly above the qualifying rows.  The projected
-- scores and score order are an independent assertion from page correctness.
-- @sortkey:0
select id, category, match(body) against('+needle' in boolean mode) as score from ft_exact where match(body) against('+needle' in boolean mode) order by score desc, id;

-- Exact BIGINT membership: the high-scoring rows are filtered out. LIMIT+OFFSET
-- must be applied after the exact membership, so the page is not under-filled.
-- These first pages intentionally keep the engine's score order (no explicit
-- ORDER BY) so the inner query retains the candidate-bound shape.  The outer
-- count checks that filtering after LIMIT+OFFSET does not under-fill the page;
-- deterministic PK pages below carry the separate row-order contract.
set fulltext_bloom_filter_pushdown = 0;
-- @sortkey:0
select count(*) as page_count from (select id from ft_exact where match(body) against('+needle' in boolean mode) and category = 'keep' limit 2 offset 1) as page;
set fulltext_bloom_filter_pushdown = 1;
-- @sortkey:0
select count(*) as page_count from (select id from ft_exact where match(body) against('+needle' in boolean mode) and category = 'keep' limit 2 offset 1) as page;
-- LIMIT without OFFSET is a second under-fill witness: an early raw bound of 2
-- would consume both excluded rows and produce a zero-row page after filtering.
-- @sortkey:0
select count(*) as page_count from (select id from ft_exact where match(body) against('+needle' in boolean mode) and category = 'keep' limit 2) as page;

-- Pagination correctness is checked with an explicit PK tie-breaker.  These
-- ordered pages are a separate SQL result contract; the candidate-bound
-- topology is asserted by the typed planner tests and the score-ordered plan
-- above, so adding the PK order does not accidentally turn a plan assertion
-- into a candidate-bound assertion.
set fulltext_bloom_filter_pushdown = 0;
-- @sortkey:0
select id from ft_exact where match(body) against('+needle' in boolean mode) and category = 'keep' order by id limit 2 offset 1;
set fulltext_bloom_filter_pushdown = 1;
-- @sortkey:0
select id from ft_exact where match(body) against('+needle' in boolean mode) and category = 'keep' order by id limit 2 offset 1;
-- @sortkey:0
select id from ft_exact where match(body) against('+needle' in boolean mode) and category = 'keep' order by id limit 0;
-- @sortkey:0
select id from ft_exact where match(body) against('+needle' in boolean mode) and category = 'keep' order by id limit 1;
-- LIMIT+OFFSET spans the qualifying set, and LIMIT larger than the set does
-- not manufacture rows.  The explicit order makes both pages deterministic.
-- @sortkey:0
select id from ft_exact where match(body) against('+needle' in boolean mode) and category = 'keep' order by id limit 2 offset 1;
-- @sortkey:0
select id from ft_exact where match(body) against('+needle' in boolean mode) and category = 'keep' order by id limit 10;

-- The inner table function receives LIMIT+OFFSET=3 on this exact pure-MUST path.
-- Keep the regex assertion focused on the semantic candidate bound rather than
-- the full explain formatting.
-- @regex("Limit: 3",true)
explain select id from ft_exact
where match(body) against('+needle' in boolean mode) and category = 'keep'
limit 2 offset 1;

-- VARCHAR membership is approximate, so the planner must not push a candidate
-- LIMIT before the final exact join. The result remains identical to pushdown OFF.
create table ft_varchar (
    id varchar(20) primary key,
    body text not null,
    category varchar(20) not null
);
insert into ft_varchar values
('doc-1', 'needle needle needle needle needle', 'drop'),
('doc-2', 'needle needle medium', 'drop'),
('doc-3', 'needle alpha', 'keep'),
('doc-4', 'needle alpha beta', 'keep'),
('doc-5', 'needle alpha beta gamma', 'keep'),
('doc-6', 'background alpha beta gamma', 'other');
create fulltext2 index ft_varchar_idx on ft_varchar(body) with parser gojieba;
set fulltext_bloom_filter_pushdown = 0;
-- @sortkey:0
select id from ft_varchar where match(body) against('+needle' in boolean mode) and category = 'keep' order by id limit 2 offset 1;
set fulltext_bloom_filter_pushdown = 1;
-- @sortkey:0
select id from ft_varchar where match(body) against('+needle' in boolean mode) and category = 'keep' order by id limit 2 offset 1;
-- @regex("Limit: 3",false)
explain select id from ft_varchar
where match(body) against('+needle' in boolean mode) and category = 'keep'
limit 2 offset 1;

-- UUID membership is also approximate and must stay unbounded at the table
-- function.  Execute the same deterministic page with both settings.  The
-- typed planner matrix owns the direct FULLTEXT2 table-function bound check.
create table ft_uuid (
    id uuid primary key,
    body text not null,
    category varchar(20) not null
);
insert into ft_uuid values
(cast('00000000-0000-0000-0000-000000000001' as uuid), 'needle needle needle needle needle', 'drop'),
(cast('00000000-0000-0000-0000-000000000002' as uuid), 'needle needle medium', 'drop'),
(cast('00000000-0000-0000-0000-000000000003' as uuid), 'needle alpha', 'keep'),
(cast('00000000-0000-0000-0000-000000000004' as uuid), 'needle alpha beta', 'keep'),
(cast('00000000-0000-0000-0000-000000000005' as uuid), 'needle alpha beta gamma', 'keep'),
(cast('00000000-0000-0000-0000-000000000006' as uuid), 'background alpha beta gamma', 'other');
create fulltext2 index ft_uuid_idx on ft_uuid(body) with parser gojieba;
set fulltext_bloom_filter_pushdown = 0;
-- @sortkey:0
select id from ft_uuid where match(body) against('+needle' in boolean mode) and category = 'keep' order by id limit 2 offset 1;
set fulltext_bloom_filter_pushdown = 1;
-- @sortkey:0
select id from ft_uuid where match(body) against('+needle' in boolean mode) and category = 'keep' order by id limit 2 offset 1;

-- A prepared pattern with a residual WHERE is not a literal pure-MUST query at
-- plan time, so it must preserve the whole source stream while returning the
-- deterministic SQL page.
prepare ft_candidate_pattern from 'select id from ft_exact where match(body) against(? in boolean mode) and category = ''keep'' order by id limit 2 offset 1';
set @candidate_pattern = '+needle';
-- @sortkey:0
execute ft_candidate_pattern using @candidate_pattern;
deallocate prepare ft_candidate_pattern;

-- Use distinct operands and columns so a future supported rewrite cannot
-- mistake this for duplicate predicates; the current unsupported shape is
-- asserted by the execution result below.
create table ft_multi (
    id bigint primary key,
    title text not null,
    body text not null,
    category varchar(20) not null
);
insert into ft_multi values
(1, 'needle', 'anchor anchor', 'drop'),
(2, 'needle', 'anchor', 'keep'),
(3, 'needle', 'anchor', 'keep'),
(4, 'needle', 'unrelated', 'keep');
create fulltext2 index ft_multi_idx on ft_multi(title, body) with parser gojieba;
-- Multiple MATCH operands over different indexed columns are rejected by the
-- current SQL rewrite because one FULLTEXT2 index cannot provide two streams.
-- Keep the unsupported shape as an execution assertion; the typed planner
-- tests cover the independent-input candidate-limit guard.
select id from ft_multi where match(title) against('+needle' in boolean mode) and match(body) against('+anchor' in boolean mode) and category = 'keep' order by id limit 2 offset 1;
-- A two-token phrase exercises positional matching rather than a one-term
-- phrase-shaped spelling.  Only the two drop rows contain this phrase.
-- @sortkey:0
select id from ft_exact where match(body) against('"needle needle"' in boolean mode) and category = 'drop' order by id limit 2;
-- Prefix, SHOULD, MUST-NOT, and ADJUST forms all execute with stable PK order.
-- @sortkey:0
select id from ft_exact where match(body) against('need*' in boolean mode) and category = 'keep' order by id limit 2 offset 1;
-- @sortkey:0
select id from ft_exact where match(body) against('+needle other' in boolean mode) and category = 'keep' order by id limit 2 offset 1;
-- @sortkey:0
select id from ft_exact where match(body) against('+(needle other)' in boolean mode) and category = 'keep' order by id limit 2 offset 1;
-- @sortkey:0
select id from ft_exact where match(body) against('+needle -other' in boolean mode) and category = 'keep' order by id limit 2 offset 1;
-- @sortkey:0
select id from ft_exact where match(body) against('+needle ~other' in boolean mode) and category = 'keep' order by id limit 2 offset 1;
-- @sortkey:0
select id from ft_exact where match(body) against('needle' in natural language mode) order by id limit 2 offset 1;
-- Execute the volatile-residual shape with an always-true predicate so the
-- result remains deterministic while the planner still sees rand() as
-- volatile and must not derive a residual-dependent candidate bound.
-- @sortkey:0
select id from ft_exact where match(body) against('+needle' in boolean mode) and category = 'keep' and rand() < 2 order by id limit 2 offset 1;

-- Non-pure Boolean forms must not receive the exact pure-MUST candidate bound.
-- @regex("Limit: 3",false)
explain select id from ft_exact
where match(body) against('+needle other' in boolean mode) and category = 'keep'
limit 2 offset 1;
-- @regex("Limit: 3",false)
explain select id from ft_exact
where match(body) against('"needle other"' in boolean mode) and category = 'keep'
limit 2 offset 1;
-- @regex("Limit: 3",false)
explain select id from ft_exact
where match(body) against('need*' in boolean mode) and category = 'keep'
limit 2 offset 1;
-- @regex("Limit: 3",false)
explain select id from ft_exact
where match(body) against('+needle -other' in boolean mode) and category = 'keep'
limit 2 offset 1;
-- @regex("Limit: 3",false)
explain select id from ft_exact
where match(body) against('+(needle other)' in boolean mode) and category = 'keep'
limit 2 offset 1;
-- @regex("Limit: 3",false)
explain select id from ft_exact
where match(body) against('+needle ~other' in boolean mode) and category = 'keep'
limit 2 offset 1;

-- A volatile residual is evaluated independently per row. It must not be
-- duplicated into the copied prefilter scan or receive a filter-dependent
-- candidate bound.
-- @regex("Limit: 3",false)
explain select id from ft_exact
where match(body) against('+needle +world' in boolean mode) and category = 'keep' and rand() < 0.5
limit 2 offset 1;

-- The existing no-residual path remains broader than the residual-WHERE fast
-- path and continues to push LIMIT+OFFSET without a membership dependency.
-- @regex("Limit: 3",true)
explain select id from ft_exact
where match(body) against('needle' in natural language mode)
limit 2 offset 1;

-- A CJK MUST operand under ngram is executed as a positional phrase, not a
-- single term, so it must not enter the pure-MUST residual-WHERE route.
create table ft_ngram (
    id bigint primary key,
    body text not null,
    category varchar(20) not null
);
insert into ft_ngram values
(1, '中文 中文', 'drop'),
(2, '中文', 'keep'),
(3, '中文', 'keep');
set experimental_fulltext2_index = 1;
create fulltext2 index ft_ngram_idx on ft_ngram(body) with parser ngram;
-- @sortkey:0
select id from ft_ngram where match(body) against('+中文' in boolean mode) and category = 'keep' order by id limit 2 offset 1;
-- @regex("Limit: 3",false)
explain select id from ft_ngram
where match(body) against('+中文' in boolean mode) and category = 'keep'
limit 2 offset 1;

-- Predicates peeled into a FULLTEXT2 INCLUDE filter are evaluated inside the
-- search rather than by an external membership filter. Preserve main's normal
-- candidate limit for this no-residual/in-index path.
create table ft_include (
    id bigint primary key,
    body text not null,
    category bigint not null,
    payload varchar(20) not null
);
insert into ft_include values
(1, 'needle needle needle', 0, 'a'),
(2, 'needle needle medium', 1, 'b'),
(3, 'needle alpha', 1, 'c'),
(4, 'needle alpha beta', 1, 'd');
set experimental_fulltext2_index = 1;
create fulltext2 index ft_include_idx on ft_include(body) include(category) with parser gojieba;
-- @sortkey:0
select id, payload from ft_include where match(body) against('needle' in natural language mode) and category = 1 order by id limit 2 offset 1;
-- @regex("Limit: 3",true)
explain select id, payload from ft_include
where match(body) against('needle' in natural language mode) and category = 1
limit 2 offset 1;

-- #30164 / #30193 / #30189 integration: the covered search keeps its score
-- bound even when the explicit score order removes the implicit score sort.
-- The eligible rows have strictly decreasing scores (2,3,4), so both pages
-- detect early OFFSET application independently of the diagnostic assertions.
select id from ft_include where match(body) against('needle' in natural language mode) and category = 1 limit 2;
select id from ft_include where match(body) against('needle' in natural language mode) and category = 1 order by match(body) against('needle' in natural language mode) desc limit 2;
select id from (select id, match(body) against('needle' in natural language mode) as sc from ft_include where match(body) against('needle' in natural language mode) and category = 1 order by sc desc limit 2) as page;
select id from (select id, match(body) against('needle' in natural language mode) as sc from ft_include where match(body) against('needle' in natural language mode) and category = 1 order by sc desc limit 2 offset 1) as page;
-- Require both optimizations: the candidate cap and a single score sort.
-- @regex("Fulltext Index Scan",true)
-- @regex("Candidate Limit: 3",true)
-- @regex("(?s)Sort Key:.*Sort Key:",false)
explain select id, match(body) against('needle' in natural language mode) as sc from ft_include where match(body) against('needle' in natural language mode) and category = 1 order by sc desc limit 2 offset 1;
-- A non-covered explicit score order bounds the search exactly like the implicit
-- relevance order when every base filter is an INCLUDE predicate the search enforces.
-- @regex("Candidate Limit: 2",true)
explain select id, payload from ft_include where match(body) against('needle' in natural language mode) and category = 1 limit 2;
-- @regex("Candidate Limit: 2",true)
explain select id, payload, match(body) against('needle' in natural language mode) as sc from ft_include where match(body) against('needle' in natural language mode) and category = 1 order by sc desc limit 2;
-- A residual payload predicate and an alternate order must still see all hits.
-- @sortkey:0
select id from ft_include where match(body) against('needle' in natural language mode) and category = 1 and payload <> 'b' order by match(body) against('needle' in natural language mode) desc limit 2;
select id from ft_include where match(body) against('needle' in natural language mode) and category = 1 order by id desc limit 2 offset 1;

-- Non-covered ORDER BY score DESC LIMIT: the score sort sits above the base-table join,
-- so the top-k is pushed into the search scan as its candidate limit.
create table ft_join (
    id bigint primary key,
    body text,
    payload varchar(10)
);
insert into ft_join values
(1, 'needle filler filler filler filler filler', 'p1'),
(2, 'needle needle filler filler filler filler', 'p2'),
(3, 'needle needle needle filler filler filler', 'p3'),
(4, 'needle needle needle needle filler filler', 'p4'),
(5, 'needle needle needle needle needle filler', 'p5'),
(6, 'other filler filler filler filler filler', 'p6');
create fulltext2 index ft_join_idx on ft_join(body);
select id, payload from ft_join where match(body) against('needle') order by match(body) against('needle') desc limit 2 offset 1;
select id, payload from (select id, payload, match(body) against('needle') as sc from ft_join where match(body) against('needle') order by sc desc limit 3) as page;
-- A residual base-table predicate declines the push; it must return the same rows.
select id, payload from ft_join where match(body) against('needle') and payload <> 'zz' order by match(body) against('needle') desc limit 2 offset 1;
-- @regex("Candidate Limit: 3",true)
explain select id, payload, match(body) against('needle') as sc from ft_join where match(body) against('needle') order by sc desc limit 2 offset 1;
-- @regex("Candidate Limit",false)
explain select id, payload, match(body) against('needle') as sc from ft_join where match(body) against('needle') and payload <> 'zz' order by sc desc limit 2 offset 1;
-- A secondary ORDER BY key declines: the search cannot choose among rows tied at the
-- cutoff by that key.
-- @regex("Candidate Limit",false)
explain select id, payload, match(body) against('needle') as sc from ft_join where match(body) against('needle') order by sc desc, id limit 2;
select id, payload from ft_join where match(body) against('needle') order by match(body) against('needle') desc, id limit 3;
select id, payload from ft_join where match(body) against('needle') and payload <> 'zz' order by match(body) against('needle') desc, id limit 3;
-- A non-score first key declines.
-- @regex("Candidate Limit",false)
explain select id, payload, match(body) against('needle') as sc from ft_join where match(body) against('needle') order by id, sc desc limit 2;
-- @regex("Candidate Limit",false)
explain select id, payload, match(body) against('needle') as sc from ft_join where match(body) against('needle') order by id desc limit 2;
-- @regex("Candidate Limit",false)
explain select id, payload, match(body) against('needle') as sc from ft_join where match(body) against('needle') order by sc asc limit 2;

-- Rows tied at the LIMIT cutoff: id 10 scores highest, ids 1-5 tie. The secondary key
-- picks among the tied rows, so ORDER BY score DESC, id is never bounded, on the covered
-- path (id only) or the base-table join path (payload).
create table ft_tie (
    id bigint primary key,
    body text,
    payload varchar(10)
);
insert into ft_tie values
(1, 'needle filler filler filler', 't1'),
(2, 'needle filler filler filler', 't2'),
(3, 'needle filler filler filler', 't3'),
(4, 'needle filler filler filler', 't4'),
(5, 'needle filler filler filler', 't5'),
(10, 'needle needle needle filler', 't10'),
(11, 'other filler filler filler', 't11');
create fulltext2 index ft_tie_idx on ft_tie(body);
-- @regex("Candidate Limit",false)
explain select id, match(body) against('needle') as sc from ft_tie where match(body) against('needle') order by sc desc, id desc limit 2;
-- @regex("Candidate Limit",false)
explain select id, payload, match(body) against('needle') as sc from ft_tie where match(body) against('needle') order by sc desc, id desc limit 2;
-- @regex("Candidate Limit",false)
explain select id, payload, match(body) against('needle') as sc from ft_tie where match(body) against('needle') order by sc desc, id limit 2 offset 1;
select id from ft_tie where match(body) against('needle') order by match(body) against('needle') desc, id desc limit 2;
select id from ft_tie where match(body) against('needle') order by match(body) against('needle') desc, id limit 2;
select id, payload from ft_tie where match(body) against('needle') order by match(body) against('needle') desc, id desc limit 2;
select id, payload from ft_tie where match(body) against('needle') order by match(body) against('needle') desc, id limit 3 offset 1;

-- Re-ordering the top n by another key: bound the search in a derived table, then sort
-- the page. The inner score-only order is bounded; the outer key orders the page.
-- @regex("Candidate Limit: 3",true)
explain select id from (select id, match(body) against('needle') as sc from ft_join where match(body) against('needle') order by sc desc limit 3) as page order by id;
-- @regex("Candidate Limit: 3",true)
explain select id, payload from (select id, payload, match(body) against('needle') as sc from ft_join where match(body) against('needle') order by sc desc limit 3) as page order by id;
select id from (select id, match(body) against('needle') as sc from ft_join where match(body) against('needle') order by sc desc limit 3) as page order by id;
select id, payload from (select id, payload, match(body) against('needle') as sc from ft_join where match(body) against('needle') order by sc desc limit 3) as page order by id desc;

-- Other shapes the bound cannot see through decline: SQL_CALC_FOUND_ROWS, an aggregate
-- and a join between the sort and the search.
-- @regex("Candidate Limit",false)
explain select sql_calc_found_rows id, match(body) against('needle') as sc from ft_join where match(body) against('needle') order by sc desc limit 2;
-- @regex("Candidate Limit",false)
explain select payload, max(match(body) against('needle')) as sc from ft_join where match(body) against('needle') group by payload order by sc desc limit 2;
-- @regex("Candidate Limit",false)
explain select j.id, t.payload, match(j.body) against('needle') as sc from ft_join j join ft_tie t on j.id = t.id where match(j.body) against('needle') order by sc desc limit 2;
select sql_calc_found_rows id from ft_join where match(body) against('needle') order by match(body) against('needle') desc limit 2;
select found_rows();
select payload, max(match(body) against('needle')) as sc from ft_join where match(body) against('needle') group by payload order by sc desc limit 2;
select j.id, t.payload from ft_join j join ft_tie t on j.id = t.id where match(j.body) against('needle') order by match(j.body) against('needle') desc, j.id limit 2;

drop database fulltext2_candidate_limit;
