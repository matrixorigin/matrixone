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
(1, 'needle needle needle needle needle', 'drop'),
(2, 'needle needle needle', 'drop'),
(3, 'needle', 'keep'),
(4, 'needle', 'keep'),
(5, 'needle', 'keep');
create fulltext2 index ft_exact_idx on ft_exact(body) with parser gojieba;

-- Exact BIGINT membership: the high-scoring rows are filtered out. LIMIT+OFFSET
-- must be applied after the exact membership, so the page is not under-filled.
set fulltext_bloom_filter_pushdown = 0;
-- @sortkey:0
select id from ft_exact
where match(body) against('+needle' in boolean mode) and category = 'keep'
limit 2 offset 1;
set fulltext_bloom_filter_pushdown = 1;
-- @sortkey:0
select id from ft_exact
where match(body) against('+needle' in boolean mode) and category = 'keep'
limit 2 offset 1;
-- @sortkey:0
select id from ft_exact
where match(body) against('+needle' in boolean mode) and category = 'keep'
limit 2;

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
('doc-2', 'needle needle needle', 'drop'),
('doc-3', 'needle', 'keep'),
('doc-4', 'needle', 'keep'),
('doc-5', 'needle', 'keep');
create fulltext2 index ft_varchar_idx on ft_varchar(body) with parser gojieba;
set fulltext_bloom_filter_pushdown = 1;
-- @sortkey:0
select id from ft_varchar
where match(body) against('+needle' in boolean mode) and category = 'keep'
limit 2 offset 1;
-- @regex("Limit: 3",false)
explain select id from ft_varchar
where match(body) against('+needle' in boolean mode) and category = 'keep'
limit 2 offset 1;

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
create fulltext2 index ft_ngram_idx on ft_ngram(body) with parser ngram;
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
create fulltext2 index ft_include_idx on ft_include(body) include(category) with parser gojieba;
-- @regex("Limit: 3",true)
explain select id, payload from ft_include
where match(body) against('needle' in natural language mode) and category = 1
limit 2 offset 1;

drop database fulltext2_candidate_limit;
