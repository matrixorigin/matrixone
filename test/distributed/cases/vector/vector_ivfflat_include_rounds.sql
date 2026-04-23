drop database if exists vector_ivfflat_include_rounds;
create database vector_ivfflat_include_rounds;
use vector_ivfflat_include_rounds;

drop table if exists include_rounds_main;
create table include_rounds_main(
    id int primary key,
    embedding vecf32(3),
    title varchar(20),
    category int
);

insert into include_rounds_main values
    (1, "[0,0,0]", "a0", 10),
    (2, "[0,0,1]", "a1", 10),
    (3, "[0,1,0]", "a2", 10),
    (4, "[1,0,0]", "a3", 10),
    (5, "[10,10,10]", "b0", 20),
    (6, "[11,11,11]", "b1", 20),
    (7, "[12,12,12]", "b2", 20),
    (8, "[13,13,13]", "b3", 20);

create index idx_ivf_include_rounds using ivfflat on include_rounds_main(embedding)
lists=2 op_type "vector_l2_ops" include(title, category);

-- `EXPLAIN ANALYZE` should stay stable across all three modes.
-- @separator:table
-- @ignore:0
explain analyze select id, title, category
from include_rounds_main
where category = 20
order by l2_distance(embedding, "[0,0,0]")
limit 2 by rank with option 'mode=include';

-- @separator:table
-- @ignore:0
explain analyze select id, title, category
from include_rounds_main
where category = 20
order by l2_distance(embedding, "[0,0,0]")
limit 2 by rank with option 'mode=post';

-- @separator:table
-- @ignore:0
explain analyze select id, title, category
from include_rounds_main
where category = 20
order by l2_distance(embedding, "[0,0,0]")
limit 2 by rank with option 'mode=pre';

-- Stable `EXPLAIN` output still distinguishes the three plan shapes.
-- @separator:table
explain select id, title, category
from include_rounds_main
where category = 20
order by l2_distance(embedding, "[0,0,0]")
limit 2 by rank with option 'mode=include';

-- @separator:table
explain select id, title, category
from include_rounds_main
where category = 20
order by l2_distance(embedding, "[0,0,0]")
limit 2 by rank with option 'mode=post';

-- @separator:table
explain select id, title, category
from include_rounds_main
where category = 20
order by l2_distance(embedding, "[0,0,0]")
limit 2 by rank with option 'mode=pre';

-- Compare the final rows after the three execution paths diverge.
-- All three modes should now recover the filtered top-k, but they do so through
-- different plan shapes: include pushes the filter into the index, post over-fetches,
-- and pre uses the extra prefilter join.
-- @separator:table
select id, title, category
from include_rounds_main
where category = 20
order by l2_distance(embedding, "[0,0,0]")
limit 2 by rank with option 'mode=include';

-- @separator:table
select id, title, category
from include_rounds_main
where category = 20
order by l2_distance(embedding, "[0,0,0]")
limit 2 by rank with option 'mode=post';

-- @separator:table
select id, title, category
from include_rounds_main
where category = 20
order by l2_distance(embedding, "[0,0,0]")
limit 2 by rank with option 'mode=pre';

drop table if exists include_rounds_residual;
create table include_rounds_residual(
    id int primary key,
    embedding vecf32(3),
    note varchar(20),
    category int
);

insert into include_rounds_residual values
    (1, "[0,0,1]", "reject", 20),
    (2, "[0,0,2]", "reject", 20),
    (3, "[0,0,3]", "reject", 20),
    (4, "[0,0,4]", "reject", 20),
    (5, "[0,0,5]", "reject", 20),
    (6, "[0,0,6]", "reject", 20),
    (7, "[0,0,7]", "reject", 20),
    (8, "[0,0,8]", "reject", 20),
    (9, "[0,0,9]", "reject", 20),
    (10, "[0,0,10]", "reject", 20),
    (11, "[0,0,11]", "reject", 20),
    (12, "[100,100,100]", "far", 20),
    (13, "[0,0,12]", "target", 20);

create index idx_ivf_include_residual using ivfflat on include_rounds_residual(embedding)
lists=1 op_type "vector_l2_ops" include(category);

-- Residual filters that are not included in the IVF payload must not cut off
-- the current centroid slice before a closer valid row is seen.
-- @separator:table
select id, note
from include_rounds_residual
where category = 20 and note = "target"
order by l2_distance(embedding, "[0,0,0]")
limit 1 by rank with option 'mode=include';

drop table include_rounds_residual;
drop table include_rounds_main;
drop database vector_ivfflat_include_rounds;
