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
(2, 'needle', 1, 'b'),
(3, 'needle', 1, 'c'),
(4, 'needle', 1, 'd');
set experimental_fulltext2_index = 1;
create fulltext2 index ft_include_idx on ft_include(body) include(category) with parser gojieba;
-- @sortkey:0
select id, payload from ft_include where match(body) against('needle' in natural language mode) and category = 1 order by id limit 2 offset 1;
-- @regex("Limit: 3",true)
explain select id, payload from ft_include
where match(body) against('needle' in natural language mode) and category = 1
limit 2 offset 1;

drop database fulltext2_candidate_limit;
