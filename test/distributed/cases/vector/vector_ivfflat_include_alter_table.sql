drop database if exists vector_ivfflat_include_phase7;
create database vector_ivfflat_include_phase7;
use vector_ivfflat_include_phase7;

drop table if exists vector_ivfflat_include_phase7_include;
create table vector_ivfflat_include_phase7_include(
    id int primary key,
    embedding vecf32(3),
    title varchar(20),
    category int,
    note varchar(20)
);

create index idx_ivf_include_phase7 using ivfflat on vector_ivfflat_include_phase7_include(embedding)
lists=2 op_type "vector_l2_ops" include(title, category);

insert into vector_ivfflat_include_phase7_include values
    (1, "[1,2,3]", "alpha", 10, "n1"),
    (2, "[1,2,4]", "beta", 20, "n2"),
    (3, "[9,9,9]", "gamma", 30, "n3"),
    (4, "[2,2,2]", "delta", 40, "n4");

show create table vector_ivfflat_include_phase7_include;
show index from vector_ivfflat_include_phase7_include;

alter table vector_ivfflat_include_phase7_include rename column title to headline;

show create table vector_ivfflat_include_phase7_include;
show index from vector_ivfflat_include_phase7_include;

-- @separator:table
explain select id, headline, category
from vector_ivfflat_include_phase7_include
order by l2_distance(embedding, "[1,2,3]")
limit 2 by rank with option 'mode=force';

select id, headline, category
from vector_ivfflat_include_phase7_include
order by l2_distance(embedding, "[1,2,3]")
limit 2 by rank with option 'mode=force';

alter table vector_ivfflat_include_phase7_include drop column category;

show create table vector_ivfflat_include_phase7_include;

-- @separator:table
explain select id, headline
from vector_ivfflat_include_phase7_include
order by l2_distance(embedding, "[1,2,3]")
limit 2 by rank with option 'mode=post';

select id, headline
from vector_ivfflat_include_phase7_include
order by l2_distance(embedding, "[1,2,3]")
limit 2 by rank with option 'mode=post';

drop table vector_ivfflat_include_phase7_include;

drop table if exists vector_ivfflat_include_phase7_plain;
create table vector_ivfflat_include_phase7_plain(
    id int primary key,
    embedding vecf32(3),
    note varchar(20)
);

create index idx_ivf_plain_phase7 using ivfflat on vector_ivfflat_include_phase7_plain(embedding)
lists=2 op_type "vector_l2_ops";

insert into vector_ivfflat_include_phase7_plain values
    (1, "[1,2,3]", "n1"),
    (2, "[1,2,4]", "n2"),
    (3, "[9,9,9]", "n3"),
    (4, "[2,2,2]", "n4");

-- @separator:table
explain select id, note
from vector_ivfflat_include_phase7_plain
where note in ("n2", "n4")
order by l2_distance(embedding, "[1,2,3]")
limit 2 by rank with option 'mode=post';

select id, note
from vector_ivfflat_include_phase7_plain
where note in ("n2", "n4")
order by l2_distance(embedding, "[1,2,3]")
limit 2 by rank with option 'mode=post';

drop table vector_ivfflat_include_phase7_plain;
drop database vector_ivfflat_include_phase7;
