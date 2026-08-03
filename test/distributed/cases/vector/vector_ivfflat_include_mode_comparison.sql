drop database if exists vector_ivfflat_include_phase9;
create database vector_ivfflat_include_phase9;
use vector_ivfflat_include_phase9;

drop table if exists phase9_main;
create table phase9_main(
    id int primary key,
    embedding vecf32(3),
    title varchar(20),
    category int,
    note varchar(20)
);

insert into phase9_main values
    (1, "[1,2,3]", "alpha", 10, "n1"),
    (2, "[1,2,4]", "beta", 20, "n2"),
    (3, "[1,2,5]", "gamma", 30, "n3"),
    (4, "[2,2,2]", "delta", 40, "n4"),
    (5, "[9,9,9]", "epsilon", 50, "n5");

create index idx_ivf_include_phase9 using ivfflat on phase9_main(embedding)
lists=2 op_type "vector_l2_ops" include(title, category);

-- Check that each mode has an IVF search path without binding the test to
-- AP/TP scheduling, runtime filters, hidden-table UUIDs, or row-count statistics.
-- @separator:table
-- @regex("Table Function on ivf_search", true)
explain select id, title, category
from phase9_main
where category >= 20
order by l2_distance(embedding, "[1,2,3]")
limit 3 by rank with option 'mode=include';

-- @separator:table
-- @regex("Table Function on ivf_search", true)
explain select id, title, category
from phase9_main
where category >= 20
order by l2_distance(embedding, "[1,2,3]")
limit 3 by rank with option 'mode=post';

-- @separator:table
-- @regex("Table Function on ivf_search", true)
explain select id, title, category
from phase9_main
where category >= 20
order by l2_distance(embedding, "[1,2,3]")
limit 3 by rank with option 'mode=pre';

-- All three modes should now recover the filtered top-k, but they do so through
-- different paths: include pushes the covered filter into the index, post keeps
-- the join path and over-fetches candidates, and pre still uses the extra
-- prefilter join.
-- @separator:table
select id, title, category
from phase9_main
where category >= 20
order by l2_distance(embedding, "[1,2,3]")
limit 3 by rank with option 'mode=include';

-- @separator:table
select id, title, category
from phase9_main
where category >= 20
order by l2_distance(embedding, "[1,2,3]")
limit 3 by rank with option 'mode=post';

-- @separator:table
select id, title, category
from phase9_main
where category >= 20
order by l2_distance(embedding, "[1,2,3]")
limit 3 by rank with option 'mode=pre';

drop table phase9_main;
drop database vector_ivfflat_include_phase9;
