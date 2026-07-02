drop database if exists vector_ivfflat_include_phase5;
create database vector_ivfflat_include_phase5;
use vector_ivfflat_include_phase5;

drop table if exists vector_ivfflat_include_phase5;
create table vector_ivfflat_include_phase5(
    id int primary key,
    embedding vecf32(3),
    title varchar(20),
    category int,
    note varchar(20)
);

create index idx_ivf_include_query using ivfflat on vector_ivfflat_include_phase5(embedding)
lists=2 op_type "vector_l2_ops" include(title, category);

insert into vector_ivfflat_include_phase5 values
    (1, "[1,2,3]", "alpha", 10, "n1"),
    (2, "[1,2,4]", "beta", 20, "n2"),
    (3, "[9,9,9]", "gamma", 30, "n3"),
    (4, "[2,2,2]", "delta", 40, "n4");

-- @separator:table
explain select id, title, category
from vector_ivfflat_include_phase5
order by l2_distance(embedding, "[1,2,3]")
limit 2 by rank with option 'mode=post';

select id, title, category
from vector_ivfflat_include_phase5
order by l2_distance(embedding, "[1,2,3]")
limit 2 by rank with option 'mode=post';

-- @separator:table
explain select id, title, category
from vector_ivfflat_include_phase5
where category >= 20
order by l2_distance(embedding, "[1,2,3]")
limit 2 by rank with option 'mode=pre';

select id, title, category
from vector_ivfflat_include_phase5
where category >= 20
order by l2_distance(embedding, "[1,2,3]")
limit 2 by rank with option 'mode=pre';

drop table vector_ivfflat_include_phase5;
drop database vector_ivfflat_include_phase5;
