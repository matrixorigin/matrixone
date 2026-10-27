-- Indexed MATCH under SQL OR/NOT. Expected rows are hand-labelled, not
-- generated from a service run. One eight-row fixture distinguishes
-- match-only, scalar-only, overlap, neither, scalar UNKNOWN, all-NULL
-- indexed content, and a matching non-NULL indexed sibling.
-- Alpha hits: 1,3,5. Beta hits: 2,3,8. tag=1: 2,3,7.
-- Distance order: 2,6,3,1,4,5,7,8; row 2 is scalar-only for alpha.
set @saved_boolean_ft = @@experimental_fulltext_index;

set @saved_boolean_ivf = @@experimental_ivf_index;

set experimental_fulltext_index = 1;

drop snapshot if exists ft_boolean_composition_sp;

drop database if exists ft_boolean_composition;

create database ft_boolean_composition;

use ft_boolean_composition;

create table docs (
    id bigint primary key,
    body text,
    alt text,
    tag int,
    v vecf32(3) not null
);

insert into docs values
    (1, 'alpha', 'blue', 0, '[3,0,0]'),
    (2, 'beta', 'green', 1, '[0,0,0]'),
    (3, 'alpha', 'beta', 1, '[2,0,0]'),
    (4, 'gamma', 'green', 0, '[4,0,0]'),
    (5, 'alpha', 'blue', NULL, '[5,0,0]'),
    (6, 'gamma', 'blue', NULL, '[1,0,0]'),
    (7, NULL, NULL, 1, '[6,0,0]'),
    (8, NULL, 'beta', 0, '[7,0,0]');

create fulltext index ft on docs(body, alt) with parser gojieba;

create index tag_idx on docs(tag);

-- Legacy controls keep positive and projection-only membership behavior.
select id from docs where match(body, alt) against('alpha' in boolean mode) order by id;

select id, (match(body, alt) against('alpha' in boolean mode) > 0) as hit from docs order by id;

set @alpha_control = (select match(body, alt) against('alpha' in boolean mode) from docs where match(body, alt) against('alpha' in boolean mode) and id = 1);

-- A named snapshot reads this same already-visible indexed generation.
-- No source DML occurs after index readiness or snapshot creation;
-- this pins Boolean composition with snapshot-bound source/score reads.
create snapshot ft_boolean_composition_sp for account;

select id from docs {snapshot='ft_boolean_composition_sp'} where match(body, alt) against('alpha' in boolean mode) or tag = 1 order by id;

drop snapshot ft_boolean_composition_sp;

-- Preserve scalar-only rows; commutation cannot change the row set.
-- All-NULL row 7 passes both OR and NOT with its non-NULL tag.
select id from docs where match(body, alt) against('alpha' in boolean mode) or tag = 1 order by id;

select id from docs where tag = 1 or match(body, alt) against('alpha' in boolean mode) order by id;

-- An OR-TRUE predicate still owns the preserving score projection.
select id, (match(body, alt) against('alpha' in boolean mode) > 0) as hit from docs where match(body, alt) against('alpha' in boolean mode) or TRUE order by id;

select id from docs where not (match(body, alt) against('alpha' in boolean mode)) and tag is not null order by id;

select id from docs where not (match(body, alt) against('alpha' in boolean mode)) and tag = 1 order by id;

-- Distinct/repeated leaves preserve one base row, including overlap row 3.
-- The last query also serves a bare positive sibling in the preserving route.
select id from docs where match(body, alt) against('alpha' in boolean mode) or match(body, alt) against('beta' in boolean mode) order by id;

select id from docs where match(body, alt) against('alpha' in boolean mode) or match(body, alt) against('alpha' in boolean mode) or tag = 1 order by id;

select id from docs where (match(body, alt) against('alpha' in boolean mode) and tag = 0) or (not (match(body, alt) against('beta' in boolean mode)) and tag = 1) order by id;

select id from docs where (match(body, alt) against('alpha' in boolean mode) or tag = 1) and match(body, alt) against('beta' in boolean mode) order by id;

-- 3VL is observable before WHERE discards UNKNOWN. The id>0 branch
-- admits all eight fixture rows without altering scalar NULLs.
select id, (match(body, alt) against('alpha' in boolean mode) or tag = 1) as either_true,
    not (match(body, alt) against('alpha' in boolean mode) or tag = 1) as negated,
    (tag = 1 and not (match(body, alt) against('alpha' in boolean mode))) as scalar_and_not
from docs where match(body, alt) against('alpha' in boolean mode) or id > 0 order by id;

-- Missing scores are exactly zero, including all-NULL row 7. Equality
-- to zero prevents the decimal rendering from hiding a tiny nonzero score.
-- An emitted score equals a positive indexed control from the same engine.
select id, cast(match(body, alt) against('alpha' in boolean mode) as decimal(10,3)) as score0,
    (match(body, alt) against('alpha' in boolean mode) = 0) as exact_zero
from docs where (match(body, alt) against('alpha' in boolean mode) or tag = 1) and id in (2,7) order by id;

select id, (match(body, alt) against('alpha' in boolean mode) = @alpha_control) as same_score
from docs where (match(body, alt) against('alpha' in boolean mode) or tag = 1) and id = 1;

select id from docs where match(body, alt) against('alpha' in boolean mode) = 0 or tag = 1 order by id;

select id from docs where match(body, alt) against('alpha' in boolean mode) > -1 or tag = 1 order by id;

select id from docs where match(body, alt) against('alpha' in boolean mode) > 1000000 or tag = 1 order by id;

select id from docs where not (match(body, alt) against('alpha' in boolean mode) > cast(NULL as double)) or tag = 1 order by id;

-- Aggregate and window inputs are the independently listed Boolean rows.
-- Both total and distinct counts detect overlap multiplication.
select count(*) as n, count(distinct id) as unique_n from docs where match(body, alt) against('alpha' in boolean mode) or match(body, alt) against('beta' in boolean mode);

select tag, count(*) as n from docs where match(body, alt) against('alpha' in boolean mode) or tag = 1
group by tag order by tag is null, tag;

select id, (match(body, alt) against('alpha' in boolean mode) > 0) as hit,
    row_number() over (order by id) as rn
from docs where match(body, alt) against('alpha' in boolean mode) or tag = 1 order by id;

-- A score-dependent PARTITION expression consumes the same served leaf.
select id, row_number() over (partition by (match(body, alt) against('alpha' in boolean mode) = 0) order by id) as rn
from docs where match(body, alt) against('alpha' in boolean mode) or tag = 1 order by id;

-- Score normalization belongs below an enclosing user LEFT join.
-- Rows 2/7 have real zero scores; missing right instance 8 remains NULL.
select p.id, q.score0 from
    (select id from docs where id in (2,7,8)) p
left join (
    select id, cast(match(body, alt) against('alpha' in boolean mode) as decimal(10,3)) as score0
    from docs where match(body, alt) against('alpha' in boolean mode) or tag = 1
) q on p.id = q.id order by p.id;

-- Pagination is global, after Boolean filtering and the explicit order.
-- Score order must admit zero-score rows ahead of positive scores.
select id from docs where match(body, alt) against('alpha' in boolean mode) or tag = 1 order by id limit 2 offset 1;

select id from docs where match(body, alt) against('alpha' in boolean mode) or tag = 1 order by match(body, alt) against('alpha' in boolean mode), id limit 2;

select id from docs where match(body, alt) against('alpha' in boolean mode) or tag = 1 order by id limit 0;

select id from docs where match(body, alt) against('alpha' in boolean mode) or tag = 1 order by id limit 2 offset 5;

-- Exact vector reference before a vector index exists. Unique distances
-- make single-key ordering deterministic and keep the ANN route reachable.
-- Row 6 is closer than every alpha hit but UNKNOWN excludes it.
select id from docs where match(body, alt) against('alpha' in boolean mode) or tag = 1 order by l2_distance(v,'[0,0,0]') limit 1;

select id from docs where match(body, alt) against('alpha' in boolean mode) or tag = 1 order by l2_distance(v,'[0,0,0]') limit 2;

select id from docs where match(body, alt) against('alpha' in boolean mode) or tag = 1 order by l2_distance(v,'[0,0,0]') limit 2 offset 1;

set experimental_ivf_index = 1;

create index v_idx using ivfflat on docs(v) lists = 1 op_type 'vector_l2_ops';

-- With the IVF index present, scalar-only closest row 2 still survives;
-- candidate truncation cannot replace the full Boolean eligibility set.
select id from docs where match(body, alt) against('alpha' in boolean mode) or tag = 1 order by l2_distance(v,'[0,0,0]') limit 1;

select id from docs where match(body, alt) against('alpha' in boolean mode) or tag = 1 order by l2_distance(v,'[0,0,0]') limit 2;

select id from docs where match(body, alt) against('alpha' in boolean mode) or tag = 1 order by l2_distance(v,'[0,0,0]') limit 2 offset 1;

-- One prepared shape is reused across term, threshold and tag changes.
-- A NULL threshold retains only the TRUE scalar branch; a negative
-- threshold admits zero scores. Returning to alpha must rebuild its hits.
prepare boolean_stmt from 'select id from docs where match(body, alt) against(? in boolean mode) > ? or tag = ? order by id';

set @term = 'alpha';

set @threshold = 0;

set @tag = 1;

execute boolean_stmt using @term, @threshold, @tag;

set @term = 'beta';

set @tag = 0;

execute boolean_stmt using @term, @threshold, @tag;

set @threshold = NULL;

execute boolean_stmt using @term, @threshold, @tag;

set @term = 'alpha';

set @threshold = -1;

set @tag = 1;

execute boolean_stmt using @term, @threshold, @tag;

set @threshold = 0;

execute boolean_stmt using @term, @threshold, @tag;

-- An ordinary absent term resets to an empty hit set without an error.
set @term = 'absentterm';

execute boolean_stmt using @term, @threshold, @tag;

-- Keep the existing engine-specific NULL-pattern contract, then reuse.
-- Classic reports invalid input even with a TRUE scalar sibling.
set @term = NULL;

execute boolean_stmt using @term, @threshold, @tag;

set @term = 'alpha';

execute boolean_stmt using @term, @threshold, @tag;

deallocate prepare boolean_stmt;

-- A term in every document, and any term of a one-row table, has idf zero, so
-- the index returns those rows with score 0. A MATCH used as a condition is
-- whether the index returned the row, so MATCH, MATCH OR FALSE and NOT MATCH
-- agree; comparisons still read the score (#31411).
create table every_doc (id bigint primary key, body text, tag int);

insert into every_doc values (1, 'qaanchor one', 0), (2, 'qaanchor two', 1), (3, 'qaanchor three', NULL);

create fulltext index ft_every on every_doc(body) with parser gojieba;

select id, match(body) against('qaanchor') as score from every_doc where match(body) against('qaanchor') order by id;

select id from every_doc where match(body) against('qaanchor') or id = -1 order by id;

select id from every_doc where not match(body) against('qaanchor') order by id;

select id from every_doc where match(body) against('+qaanchor' in boolean mode) or tag = 1 order by id;

select id from every_doc where not (match(body) against('+qaanchor' in boolean mode)) and tag is not null order by id;

select id, case when match(body) against('qaanchor') then 'y' else 'n' end as hit from every_doc where match(body) against('qaanchor') or id > 0 order by id;

select id, if(match(body) against('qaanchor'), 'y', 'n') as hit from every_doc where match(body) against('qaanchor') order by id;

select id, if(match(body) against('qaanchor'), 'y', 'n') as hit from every_doc where match(body) against('qaanchor') or id = -1 order by id;

select count(if(match(body) against('qaanchor'), 1, null)) as hits from every_doc where match(body) against('qaanchor') or id = -1;

-- CAST(MATCH AS BOOL) is whether the index returned the row; a cast through DOUBLE converts the score.
select id, cast(match(body) against('qaanchor') as bool) as b, cast(cast(match(body) against('qaanchor') as double) as bool) as d from every_doc where match(body) against('qaanchor') order by id;

select id, cast(match(body) against('qaanchor') as bool) as b from every_doc where match(body) against('qaanchor') or id = -1 order by id;

select id from every_doc where match(body) against('qaanchor') = 0 or id = -1 order by id;

select id from every_doc where (match(body) against('qaanchor') and tag = 1) or id = -1 order by id;

select id from every_doc where match(body) against('qaanchor') xor tag = 1 order by id;

select id from every_doc where !match(body) against('qaanchor') order by id;

select id, match(body) against('qaanchor') is true as t, round(match(body) against('qaanchor'), 3) as r from every_doc where match(body) against('qaanchor') or id = -1 order by id;

select id from every_doc where match(body) against('one') or not match(body) against('qaanchor') order by id;

-- A projected score is the score, also when a condition uses it.
select id from (select id, match(body) against('qaanchor') as sc from every_doc where match(body) against('qaanchor') or id > 0) d where sc order by id;

select id from (select id, match(body) against('qaanchor') as sc from every_doc where match(body) against('qaanchor')) d where sc or id = -1 order by id;

select id, if(sc, 'y', 'n') as hit from (select id, match(body) against('qaanchor') as sc from every_doc where match(body) against('qaanchor')) d where not sc order by id;

create table one_doc (id bigint primary key, body text);

insert into one_doc values (1, 'solo');

create fulltext index ft_one on one_doc(body) with parser gojieba;

select id from one_doc where match(body) against('solo');

select id from one_doc where match(body) against('solo') or false;

select id from one_doc where not match(body) against('solo');

-- Index coverage/binding refusal is all-or-nothing, even if another
-- leaf is valid or the scalar branch alone could produce rows.
select id from docs where match(body, alt) against('alpha' in boolean mode) or match(body) against('beta' in boolean mode) or tag = 1 order by id;

select a.id from docs a join docs b on a.id = b.id where match(a.body, b.alt) against('alpha' in boolean mode) or a.tag = 1 order by a.id;

drop index ft on docs;

select id from docs where match(body, alt) against('alpha' in boolean mode) or tag = 1 order by id;

drop database ft_boolean_composition;

set experimental_fulltext_index = @saved_boolean_ft;

set experimental_ivf_index = @saved_boolean_ivf;
